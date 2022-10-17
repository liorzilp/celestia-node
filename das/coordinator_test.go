package das

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-node/header"

	"github.com/stretchr/testify/assert"
)

func TestCoordinator(t *testing.T) {

	params := dasingParams{}
	params.samplingRange = 10
	params.concurrencyLimit = 10
	params.bgStoreInterval = 10 * time.Minute
	params.priorityQueueSize = 16 * 4
	params.genesisHeight = 1

	networkHead := uint64(500)
	sampleFrom := uint64(params.genesisHeight)
	timeoutDelay := 125 * time.Second

	t.Run("test run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleFrom, networkHead)

		coordinator := newSamplingCoordinator(params, getterStub{}, onceMiddleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		// check if all jobs were sampled successfully
		assert.NoError(t, sampler.finished(ctx), "not all headers were sampled")

		// wait for coordinator to indicateDone catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list should be empty")

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("discovered new headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleFrom, networkHead)

		coordinator := newSamplingCoordinator(params, getterStub{}, sampler.sample)
		go coordinator.run(ctx, sampler.checkpoint)

		time.Sleep(50 * time.Millisecond)
		// discover new height
		for i := 0; i < 200; i++ {
			// mess the order by running in go-routine
			sampler.discover(ctx, networkHead+uint64(i), coordinator.listen)
		}

		// check if all jobs were sampled successfully
		assert.NoError(t, sampler.finished(ctx), "not all headers were sampled")

		// wait for coordinator to indicateDone catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list should be empty")

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("prioritize newly discovered over known", func(t *testing.T) {

		// params := dasingParams{}
		params.concurrencyLimit = 1
		params.samplingRange = 4

		sampleFrom := uint64(1)
		networkHead := uint64(10)
		toBeDiscovered := uint64(20)

		sampler := newMockSampler(sampleFrom, networkHead)

		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		// lock worker before start, to not let it indicateDone before discover
		lk := newLock(sampleFrom, sampleFrom)

		// expect worker to prioritize newly discovered  (20 -> 10) and then old (0 -> 10)
		order := newCheckOrder().addInterval(sampleFrom, uint64(params.samplingRange)) // worker will pick up first job before discovery
		order.addStacks(networkHead+1, toBeDiscovered, uint64(params.samplingRange))
		order.addInterval(uint64(params.samplingRange+1), toBeDiscovered)

		// start coordinator
		coordinator := newSamplingCoordinator(params, getterStub{},
			lk.middleWare(
				order.middleWare(sampler.sample),
			),
		)
		go coordinator.run(ctx, sampler.checkpoint)

		// wait for worker to pick up first job
		time.Sleep(50 * time.Millisecond)

		// discover new height
		sampler.discover(ctx, toBeDiscovered, coordinator.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, sampler.sampledAmount())

		// unblock worker
		lk.release(sampleFrom)

		// check if all jobs were sampled successfully
		assert.NoError(t, sampler.finished(ctx), "not all headers were sampled")

		// wait for coordinator to indicateDone catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list should be empty")

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("priority routine should not lock other workers", func(t *testing.T) {
		params.samplingRange = 20
		networkHead := uint64(20)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleFrom, networkHead)

		lk := newLock(sampleFrom, networkHead) // lock all workers before start
		coordinator := newSamplingCoordinator(params, getterStub{},
			lk.middleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		time.Sleep(50 * time.Millisecond)
		// discover new height and lock it
		discovered := networkHead + 1
		lk.add(discovered)
		sampler.discover(ctx, discovered, coordinator.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, sampler.sampledAmount())

		// unblock workers to resume sampling
		lk.releaseAll(discovered)

		// wait for coordinator to run sample on all headers except discovered
		time.Sleep(100 * time.Millisecond)

		// check that only last header is pending
		assert.EqualValues(t, int(discovered-sampleFrom), sampler.doneAmount())
		assert.False(t, sampler.heightIsDone(discovered))

		// release all headers for coordinator
		lk.releaseAll()

		// check if all jobs were sampled successfully
		assert.NoError(t, sampler.finished(ctx), "not all headers were sampled")

		// wait for coordinator to indicateDone catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list is not empty")

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("failed should be stored", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		bornToFail := []uint64{4, 8, 15, 16, 23, 42}
		sampler := newMockSampler(sampleFrom, networkHead, bornToFail...)

		coordinator := newSamplingCoordinator(params, getterStub{}, onceMiddleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		// wait for coordinator to indicateDone catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.wait(stopCtx))

		// set failed items in expectedState
		expectedState := sampler.finalState()
		for _, h := range bornToFail {
			expectedState.Failed[h] = 1
		}
		assert.Equal(t, expectedState, newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("failed should retry on restart", func(t *testing.T) {
		sampleFrom := uint64(50)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		failedLastRun := map[uint64]int{4: 1, 8: 2, 15: 1, 16: 1, 23: 1, 42: 1, sampleFrom - 1: 1}
		failedAgain := []uint64{16}

		sampler := newMockSampler(sampleFrom, networkHead, failedAgain...)
		sampler.checkpoint.Failed = failedLastRun

		coordinator := newSamplingCoordinator(params, getterStub{}, onceMiddleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		// check if all jobs were sampled successfully
		assert.NoError(t, sampler.finished(ctx), "not all headers were sampled")

		// wait for coordinator to indicateDone catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.wait(stopCtx))

		expectedState := sampler.finalState()
		expectedState.Failed = make(map[uint64]int)
		for _, v := range failedAgain {
			expectedState.Failed[v] = failedLastRun[v] + 1
		}
		assert.Equal(t, expectedState, newCheckpoint(coordinator.state.unsafeStats()))
	})
}

func BenchmarkCoordinator(b *testing.B) {
	timeoutDelay := 5 * time.Second

	params := dasingParams{}
	params.samplingRange = 10
	params.concurrencyLimit = 100
	params.bgStoreInterval = 10 * time.Minute
	params.priorityQueueSize = 16 * 4
	params.genesisHeight = 1

	b.Run("bench run", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		coordinator := newSamplingCoordinator(params, newBenchGetter(),
			func(ctx context.Context, h *header.ExtendedHeader) error { return nil })
		go coordinator.run(ctx, checkpoint{
			SampleFrom:  1,
			NetworkHead: uint64(b.N),
		})

		// wait for coordinator to indicateDone catchup
		if err := coordinator.state.waitCatchUp(ctx); err != nil {
			b.Error(err)
		}
		cancel()
	})
}

// ensures all headers are sampled in range except ones that are born to fail
type mockSampler struct {
	lock sync.Mutex

	checkpoint
	bornToFail map[uint64]bool
	done       map[uint64]int

	isFinished bool
	finishedCh chan struct{}
}

func newMockSampler(sampledBefore, sampleTo uint64, bornToFail ...uint64) mockSampler {
	failMap := make(map[uint64]bool)
	for _, h := range bornToFail {
		failMap[h] = true
	}
	return mockSampler{
		checkpoint: checkpoint{
			SampleFrom:  sampledBefore,
			NetworkHead: sampleTo,
			Failed:      make(map[uint64]int),
			Workers:     make([]workerCheckpoint, 0),
		},
		bornToFail: failMap,
		done:       make(map[uint64]int),
		finishedCh: make(chan struct{}),
	}
}

func (m *mockSampler) sample(ctx context.Context, h *header.ExtendedHeader) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	height := uint64(h.Height)
	m.done[height]++

	if len(m.done) > int(m.NetworkHead-m.SampleFrom) && !m.isFinished {
		m.isFinished = true
		close(m.finishedCh)
	}

	if m.bornToFail[height] {
		return errors.New("born to fail, sad life")
	}

	if height > m.NetworkHead || height < m.SampleFrom {
		if m.Failed[height] == 0 {
			return fmt.Errorf("header: %v out of range: %v-%v", h, m.SampleFrom, m.NetworkHead)
		}
	}
	return nil
}

// finished returns when all jobs were sampled successfully
func (m *mockSampler) finished(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.finishedCh:
	}
	return nil
}

func (m *mockSampler) heightIsDone(h uint64) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.done[h] != 0
}

func (m *mockSampler) doneAmount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.done)
}

func (m *mockSampler) finalState() checkpoint {
	m.lock.Lock()
	defer m.lock.Unlock()

	finalState := m.checkpoint
	finalState.SampleFrom = finalState.NetworkHead + 1
	return finalState
}

func (m *mockSampler) discover(ctx context.Context, newHeight uint64, emit func(ctx context.Context, h uint64)) {
	m.lock.Lock()

	if newHeight > m.checkpoint.NetworkHead {
		m.checkpoint.NetworkHead = newHeight
		if m.isFinished {
			m.finishedCh = make(chan struct{})
			m.isFinished = false
		}
	}
	m.lock.Unlock()
	emit(ctx, newHeight)
}

func (m *mockSampler) sampledAmount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.done)
}

// ensures correct order of operations
type checkOrder struct {
	lock  sync.Mutex
	queue []uint64
}

func newCheckOrder() *checkOrder {
	return &checkOrder{}
}

func (o *checkOrder) addInterval(start, end uint64) *checkOrder {
	o.lock.Lock()
	defer o.lock.Unlock()

	if end > start {
		for end >= start {
			o.queue = append(o.queue, start)
			start++
		}
		return o
	}

	for start >= end {
		o.queue = append(o.queue, start)
		if start == 0 {
			return o
		}
		start--

	}
	return o
}

// splits interval into ranges with stackSize length and puts them with reverse order
func (o *checkOrder) addStacks(start, end, stackSize uint64) uint64 {
	if start+stackSize-1 < end {
		end = o.addStacks(start+stackSize, end, stackSize)
	}
	if start > end {
		start = end
	}
	o.addInterval(start, end)
	return start - 1
}

func TestOrder(t *testing.T) {
	o := newCheckOrder().addInterval(0, 3).addInterval(3, 0)
	assert.Equal(t, []uint64{0, 1, 2, 3, 3, 2, 1, 0}, o.queue)
}

func TestStack(t *testing.T) {
	o := newCheckOrder()
	o.addStacks(10, 20, 3)
	assert.Equal(t, []uint64{19, 20, 16, 17, 18, 13, 14, 15, 10, 11, 12}, o.queue)
}

func (o *checkOrder) middleWare(out sampleFn) sampleFn {
	return func(ctx context.Context, h *header.ExtendedHeader) error {
		o.lock.Lock()

		if len(o.queue) > 0 {
			// check last item in queue to be same as input
			if o.queue[0] != uint64(h.Height) {
				o.lock.Unlock()
				return fmt.Errorf("expected height: %v,got: %v", o.queue[0], h)
			}
			o.queue = o.queue[1:]
		}

		o.lock.Unlock()
		return out(ctx, h)
	}
}

// blocks operations if item is in lock list
type lock struct {
	m         sync.Mutex
	blockList map[uint64]chan struct{}
}

func newLock(from, to uint64) *lock {
	list := make(map[uint64]chan struct{})
	for from <= to {
		list[from] = make(chan struct{})
		from++
	}
	return &lock{
		blockList: list,
	}
}

func (l *lock) add(hs ...uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	for _, h := range hs {
		l.blockList[h] = make(chan struct{})
	}
}

func (l *lock) release(hs ...uint64) {
	l.m.Lock()
	defer l.m.Unlock()

	for _, h := range hs {
		if ch, ok := l.blockList[h]; ok {
			close(ch)
			delete(l.blockList, h)
		}
	}
}

func (l *lock) releaseAll(except ...uint64) {
	m := make(map[uint64]bool)
	for _, h := range except {
		m[h] = true
	}

	l.m.Lock()
	defer l.m.Unlock()

	for h, ch := range l.blockList {
		if m[h] {
			continue
		}
		close(ch)
		delete(l.blockList, h)
	}
}

func (l *lock) middleWare(out sampleFn) sampleFn {
	return func(ctx context.Context, h *header.ExtendedHeader) error {
		l.m.Lock()
		ch, blocked := l.blockList[uint64(h.Height)]
		l.m.Unlock()
		if !blocked {
			return out(ctx, h)
		}

		select {
		case <-ch:
			return out(ctx, h)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func onceMiddleWare(out sampleFn) sampleFn {
	db := make(map[int64]int)
	m := sync.Mutex{}
	return func(ctx context.Context, h *header.ExtendedHeader) error {
		m.Lock()
		db[h.Height]++
		if db[h.Height] > 1 {
			m.Unlock()
			return fmt.Errorf("header sampled more than once: %v", h.Height)
		}
		m.Unlock()
		return out(ctx, h)
	}
}
