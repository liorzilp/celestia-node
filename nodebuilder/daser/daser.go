package daser

import (
	"github.com/ipfs/go-datastore"

	"github.com/celestiaorg/celestia-node/das"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/nodebuilder/fraud"
	"github.com/celestiaorg/celestia-node/share"
)

func NewDASer(
	da share.Availability,
	hsub header.Subscriber,
	store header.Store,
	batching datastore.Batching,
	fraudService fraud.Module,
	options ...das.Option,
) *das.DASer {
	return das.NewDASer(da, hsub, store, batching, fraudService, options...)
}
