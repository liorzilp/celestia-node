package daser

import (
	"context"

	"go.uber.org/fx"

	"github.com/celestiaorg/celestia-node/das"
	"github.com/celestiaorg/celestia-node/fraud"
	"github.com/celestiaorg/celestia-node/libs/fxutil"
	fraudServ "github.com/celestiaorg/celestia-node/nodebuilder/fraud"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
)

func ConstructModule(tp node.Type, cfg *Config) fx.Option {

	cfgErr := cfg.Validate()

	baseComponents := fx.Options(
		fx.Supply(*cfg),
		fx.Error(cfgErr),
		fx.Provide(
			func(c Config) []das.Option {
				return []das.Option{
					das.WithParamSamplingRange(int(c.SamplingRange)),
					das.WithParamConcurrencyLimit(int(c.ConcurrencyLimit)),
					das.WithParamPriorityQueueSize(int(c.PriorityQueueSize)),
					das.WithParamBackgroundStoreInterval(c.BackgroundStoreInterval),
				}
			},
		),
	)

	switch tp {
	case node.Light, node.Full:
		return fx.Module(
			"daser",
			baseComponents,
			fx.Provide(
				fx.Annotate(
					NewDASer,
					fx.OnStart(func(ctx context.Context, lc fx.Lifecycle, fservice fraudServ.Module, das *das.DASer) error {
						lifecycleCtx := fxutil.WithLifecycle(ctx, lc)
						return fraudServ.Lifecycle(ctx, lifecycleCtx, fraud.BadEncoding, fservice, das.Start, das.Stop)
					}),
					fx.OnStop(func(ctx context.Context, das *das.DASer) error {
						return das.Stop(ctx)
					}),
				),
			),
		)
	case node.Bridge:
		return fx.Options()
	default:
		panic("invalid node type")
	}
}
