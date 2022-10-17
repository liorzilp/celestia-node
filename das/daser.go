package das

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/celestiaorg/celestia-node/ipld"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	"github.com/celestiaorg/celestia-node/fraud"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
)

// TODO(@team): Should we DI this?
var log = logging.Logger("das")

type dasingParams struct {
	samplingRange     uint
	concurrencyLimit  uint
	bgStoreInterval   time.Duration
	priorityQueueSize uint
	genesisHeight     uint
}

type Option func(*DASer)

func WithParamSamplingRange(samplingRange int) Option {
	return func(d *DASer) {
		d.params.samplingRange = uint(samplingRange)
	}
}

func WithParamConcurrencyLimit(concurrencyLimit int) Option {
	return func(d *DASer) {
		d.params.concurrencyLimit = uint(concurrencyLimit)
	}
}

func WithParamBackgroundStoreInterval(bgStoreInterval time.Duration) Option {
	return func(d *DASer) {
		d.params.bgStoreInterval = bgStoreInterval
	}
}

func WithParamPriorityQueueSize(priorityQueueSize int) Option {
	return func(d *DASer) {
		d.params.priorityQueueSize = uint(priorityQueueSize)
	}
}

func WithParamGenesisHeight(genesisHeight uint) Option {
	return func(d *DASer) {
		d.params.genesisHeight = genesisHeight
	}
}

// DASer continuously validates availability of data committed to headers.
type DASer struct {
	params dasingParams

	da     share.Availability
	bcast  fraud.Broadcaster
	hsub   header.Subscriber // listens for new headers in the network
	getter header.Getter     // retrieves past headers

	sampler    *samplingCoordinator
	store      checkpointStore
	subscriber subscriber

	cancel         context.CancelFunc
	subscriberDone chan struct{}
	running        int32
}

type listenFn func(ctx context.Context, height uint64)
type sampleFn func(context.Context, *header.ExtendedHeader) error

// NewDASer creates a new DASer.
func NewDASer(
	da share.Availability,
	hsub header.Subscriber,
	getter header.Getter,
	dstore datastore.Datastore,
	bcast fraud.Broadcaster,
	options ...Option,
) *DASer {
	d := &DASer{
		params:         dasingParams{},
		da:             da,
		bcast:          bcast,
		hsub:           hsub,
		getter:         getter,
		store:          newCheckpointStore(dstore),
		subscriber:     newSubscriber(),
		subscriberDone: make(chan struct{}),
	}

	for _, applyOpt := range options {
		applyOpt(d)
	}

	if len(options) == 0 {
		log.Warn("das/daser: no options applied, daser parameters will be instantiated with zero-values, make sure to configure the daser")
	}

	d.sampler = newSamplingCoordinator(d.params, getter, d.sample)

	return d
}

// Start initiates subscription for new ExtendedHeaders and spawns a sampling routine.
func (d *DASer) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&d.running, 0, 1) {
		return fmt.Errorf("da: DASer already started")
	}

	sub, err := d.hsub.Subscribe()
	if err != nil {
		return err
	}

	// load latest DASed checkpoint
	cp, err := d.store.load(ctx)
	if err != nil {
		log.Warnw("checkpoint not found, initializing with height 1")

		cp = checkpoint{
			SampleFrom:  uint64(d.params.genesisHeight),
			NetworkHead: uint64(d.params.genesisHeight),
		}

		// attempt to get head info. No need to handle error, later DASer
		// will be able to find new head from subscriber after it is started
		if h, err := d.getter.Head(ctx); err == nil {
			cp.NetworkHead = uint64(h.Height)
		}
	}
	log.Info("starting DASer from checkpoint: ", cp.String())

	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go d.sampler.run(runCtx, cp)
	go d.subscriber.run(runCtx, sub, d.sampler.listen)
	go d.store.runBackgroundStore(runCtx, d.params.bgStoreInterval, d.sampler.getCheckpoint)

	return nil
}

// Stop stops sampling.
func (d *DASer) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		return nil
	}

	// try to store checkpoint without waiting for coordinator and workers to stop
	cp, err := d.sampler.getCheckpoint(ctx)
	if err != nil {
		log.Error("DASer coordinator checkpoint is unavailable")
	}

	if err = d.store.store(ctx, cp); err != nil {
		log.Errorw("storing checkpoint to disk", "err", err)
	}

	d.cancel()
	if err = d.sampler.wait(ctx); err != nil {
		return fmt.Errorf("DASer force quit: %w", err)
	}

	// save updated checkpoint after sampler and all workers are shut down
	if err = d.store.store(ctx, newCheckpoint(d.sampler.state.unsafeStats())); err != nil {
		log.Errorw("storing checkpoint to disk", "Err", err)
	}

	if err = d.store.wait(ctx); err != nil {
		return fmt.Errorf("DASer force quit with err: %w", err)
	}
	return d.subscriber.wait(ctx)
}

func (d *DASer) sample(ctx context.Context, h *header.ExtendedHeader) error {
	err := d.da.SharesAvailable(ctx, h.DAH)
	if err != nil {
		if err == context.Canceled {
			return err
		}
		var byzantineErr *ipld.ErrByzantine
		if errors.As(err, &byzantineErr) {
			log.Warn("Propagating proof...")
			sendErr := d.bcast.Broadcast(ctx, fraud.CreateBadEncodingProof(h.Hash(), uint64(h.Height), byzantineErr))
			if sendErr != nil {
				log.Errorw("fraud proof propagating failed", "err", sendErr)
			}
		}

		log.Errorw("sampling failed", "height", h.Height, "hash", h.Hash(),
			"square width", len(h.DAH.RowsRoots), "data root", h.DAH.Hash(), "err", err)
		return err
	}

	return nil
}

func (d *DASer) SamplingStats(ctx context.Context) (SamplingStats, error) {
	return d.sampler.stats(ctx)
}
