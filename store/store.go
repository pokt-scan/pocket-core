package store

import (
	"github.com/pokt-network/pocket-core/store/rootmulti"
	dbm "github.com/tendermint/tm-db"

	"github.com/pokt-network/pocket-core/store/types"
)

func NewCommitMultiStore(db dbm.DB, datadir string) types.CommitMultiStore {
	return rootmulti.NewMultiStore(db, datadir, 100000)
}
