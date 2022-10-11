package daser

import (
	"errors"
	"time"
)

var (
	ErrNegativeInterval = errors.New("nodebuilder/daser: interval must be positive")
)

type Config struct {
	//  samplingRange is the maximum amount of headers processed in one job.
	SamplingRange uint64 // TODO(@derrandz): Question to @vlad, why is this uint64?

	// concurrencyLimit defines the maximum amount of sampling workers running in parallel.
	ConcurrencyLimit uint

	// backgroundStoreInterval is the period of time for background checkpointStore to perform a checkpoint backup.
	BackgroundStoreInterval time.Duration

	// priorityQueueSize defines the size limit of the priority queue
	PriorityQueueSize uint

	// genesisHeight is the height sampling will start from
	GenesisHeight uint
}

// TODO(@derrandz): parameters needs performance testing on real network to define optimal values
func DefaultConfig() Config {
	return Config{
		SamplingRange:           100,
		ConcurrencyLimit:        16,
		BackgroundStoreInterval: 10 * time.Minute,
		PriorityQueueSize:       16 * 4,
		GenesisHeight:           1,
	}
}

// Validate performs basic validation of the config.
func (cfg *Config) Validate() error {
	if cfg.BackgroundStoreInterval <= 0 {
		return ErrNegativeInterval
	}
	return nil
}
