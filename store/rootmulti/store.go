package rootmulti

import (
	"fmt"
	"github.com/pokt-network/pocket-core/store/cachemulti"
	"github.com/pokt-network/pocket-core/store/iavl"
	"github.com/pokt-network/pocket-core/store/rootmulti/appstatedb"
	"github.com/pokt-network/pocket-core/store/types"
	dbm "github.com/tendermint/tm-db"
	"log"
)

// Prefixed abstractions living inside AppDB;
type Store struct {
	asdb                  *appstatedb.AppStateDB // Inmutable app state database
	appDB                 dbm.DB                 // parent db, (where everything lives except for state)
	state                 dbm.DB                 // ephemeral state used to 'stage' potential changes; only latest height; nuked on startup;
	iavl                  *iavl.Store            // used for latest height state commitments ONLY; may be pruned;
	storeKey              string                 // constant; part of the prefix
	height                int64                  // dynamic; part of the prefix
	isMutable             bool                   // !isReadOnly
	immutableLatestHeight int64                  // READ ONLY IMMUTABLE LATEST KNOWN HEIGHT
	pruneDepth            int64
}

func NewStore(appDB dbm.DB, height int64, storeKey string, commitID types.CommitID, stateDir string, baseDatadir string, isMutable bool, immutableLatestHeight int64) *Store {
	store := &Store{
		appDB:     appDB,
		storeKey:  storeKey,
		isMutable: isMutable,
		height:    height,
	}
	if isMutable {
		// load height-1 into state from AppDB
		prefix := StoreKey(height-1, storeKey, "")
		it, err := appDB.Iterator(prefix, types.PrefixEndBytes(prefix))
		if err != nil {
			panic(fmt.Sprintf("unable to create an iterator for height %d storeKey %s", height, storeKey))
		}
		defer it.Close()
		store.state, err = dbm.NewGoLevelDB(storeKey, stateDir)
		if err != nil {
			panic(err)
		}
		for ; it.Valid(); it.Next() {
			err := store.state.Set(KeyFromStoreKey(it.Key()), it.Value())
			if err != nil {
				panic("unable to set k/v in state: " + err.Error())
			}
		}
		// load IAVL from AppDB
		store.iavl, err = iavl.LoadStore(dbm.NewPrefixDB(appDB, []byte("s/k:"+storeKey+"/")), commitID, false)
		if err != nil {
			panic("unable to load iavlStore in rootmultistore: " + err.Error())
		}
	}
	
	// Load the app state db for this store
	asdb, asdbErr := appstatedb.NewAppStateDB(baseDatadir, storeKey)
	if asdbErr != nil {
		panic("Unable to load app statedb in store: " + asdbErr.Error())
	}
	store.asdb = asdb

	return store
}

func (is *Store) getLatestHeight() int64 {
	if is.isMutable {
		return is.height
	} else {
		return is.immutableLatestHeight
	}
}

func (is *Store) isHistoricalQuery() bool {
	if is.isMutable {
		panic("Shouldn't be called if mutable")
	}

	return is.getLatestHeight()-is.pruneDepth < is.height
}

func (is *Store) LoadImmutableVersion(version int64, stateDir string, baseDatadir string, immutableLatestHeight int64) *Store {
	return NewStore(is.appDB, version, is.storeKey, types.CommitID{}, stateDir, baseDatadir, false, immutableLatestHeight)
}

func (is *Store) Get(key []byte) ([]byte, error) {
	if is.isMutable { // if latestHeight
		return is.state.Get(key)
	}

	if is.isHistoricalQuery() {
		return is.asdb.GetMutable(is.height-1, is.storeKey, key)
	} else {
		return is.appDB.Get(StoreKey(is.height-1, is.storeKey, string(key)))
	}
}

func (is *Store) Has(key []byte) (bool, error) {
	if is.isMutable { // if latestHeight
		return is.state.Has(key)
	}

	if is.isHistoricalQuery() {
		return is.asdb.HasMutable(is.height-1, is.storeKey, key)
	} else {
		return is.appDB.Has(StoreKey(is.height-1, is.storeKey, string(key)))
	}
}

func (is *Store) Set(key, value []byte) error {
	if is.isMutable {
		// Set the Iavl
		err := is.iavl.Set(key, value)
		if err != nil {
			panic("unable to set to iavl: " + err.Error())
		}

		// Set the state db
		stateErr := is.state.Set(key, value)
		if stateErr != nil {
			panic("unable to set state: " + stateErr.Error())
		}

		// Return with the historical set
		asdbErr := is.asdb.SetMutable(is.height, is.storeKey, key, value)
		if asdbErr != nil {
			panic("unable to set to asdb: " + asdbErr.Error())
		}
		return asdbErr
	}
	panic("'Set()' called on immutable store")
}

func (is *Store) Delete(key []byte) error {
	if is.isMutable {
		err := is.iavl.Delete(key)
		if err != nil {
			panic("unable to delete to iavl: " + err.Error())
		}
		// Delete the state db
		stateErr := is.state.Delete(key)
		if stateErr != nil {
			panic("unable to set state: " + stateErr.Error())
		}

		// Return with the historical delete
		asdbErr := is.asdb.DeleteMutable(is.height, is.storeKey, key)
		if asdbErr != nil {
			panic("unable to set to asdb: " + asdbErr.Error())
		}
		return asdbErr
	}
	panic("'Delete()' called on immutable store")
}

func (is *Store) Iterator(start, end []byte) (types.Iterator, error) {
	if is.isMutable {
		return is.state.Iterator(start, end)
	}

	if is.isHistoricalQuery() {
		return is.asdb.IteratorMutable(is.height - 1, is.storeKey, start, end)
	} else {
		baseIterator, err := is.appDB.Iterator(StoreKey(is.height-1, is.storeKey, string(start)), StoreKey(is.height-1, is.storeKey, string(end)))
		return AppDBIterator{it: baseIterator}, err
	}

}

func (is *Store) ReverseIterator(start, end []byte) (types.Iterator, error) {
	if is.isMutable {
		return is.state.ReverseIterator(start, end)
	}
	if is.isHistoricalQuery() {
		return is.asdb.ReverseIteratorMutable(is.height - 1, is.storeKey, start, end)
	} else {
		baseIterator, err := is.appDB.ReverseIterator(StoreKey(is.height-1, is.storeKey, string(start)), StoreKey(is.height-1, is.storeKey, string(end)))
		return AppDBIterator{it: baseIterator}, err
	}
}

// Persist State & IAVL
func (is *Store) CommitBatch(b dbm.Batch) (commitID types.CommitID, batch dbm.Batch) {
	// commit iavl
	commitID = is.iavl.Commit()
	// commit entire state
	it, err := is.state.Iterator(nil, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to create an iterator for height %d storeKey %s in Commit()", is.height, is.storeKey))
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		b.Set(StoreKey(is.height, is.storeKey, string(it.Key())), it.Value())
	}

	// Commit to the historical db (asdb)
	commitErr := is.asdb.CommitMutable()
	if commitErr != nil {
		panic(commitErr.Error())
	}

	is.height++

	if is.height >= 300 {
		log.Fatal("TELMINAMO")
	}

	return commitID, b
}

// Prune version in IAVL & AppDB
func (is *Store) PruneVersion(batch dbm.Batch, version int64) dbm.Batch {
	// iavl
	is.iavl.DeleteVersion(version)
	// appDB
	prefix := StoreKey(version, is.storeKey, "")
	it, err := is.appDB.Iterator(prefix, types.PrefixEndBytes(prefix))
	if err != nil {
		panic("unable to create iterator in PruneVersion for appDB")
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		batch.Delete(it.Key())
	}
	return batch
}

func (is *Store) LastCommitID() types.CommitID {
	if is.isMutable {
		return is.iavl.LastCommitID()
	}
	panic("LastCommitID for called on an immutable store")
}

func (is *Store) CacheWrap() types.CacheWrap {
	return cachemulti.NewStoreCache(is)
}

func (is *Store) GetStoreType() types.StoreType {
	return types.StoreTypeDefault
}

func (is *Store) Commit() types.CommitID {
	panic("use CommitBatch for atomic safety")
}

var _ types.CommitKVStore = &Store{}

func StoreKey(height int64, store string, key string) []byte {
	height += 1
	if store == "" {
		return []byte(fmt.Sprintf("%d/", height))
	}
	if key == "" {
		return []byte(fmt.Sprintf("%d/%s/", height, store))
	}
	return []byte(fmt.Sprintf("%d/%s/%s", height, store, key))
}

func KeyFromStoreKey(storeKey []byte) (key []byte) {
	delim := 0
	for i, b := range storeKey {
		if b == byte('/') {
			delim++
		}
		if delim == 2 {
			return storeKey[i+1:]
		}
	}
	panic("attempted to get key from store key that doesn't have exactly 2 delims")
}

var _ dbm.Iterator = AppDBIterator{}

// Is 'height/storeKey' aware
type AppDBIterator struct {
	it dbm.Iterator
}

func (s AppDBIterator) Key() (key []byte) {
	return KeyFromStoreKey(s.it.Key())
}

func (s AppDBIterator) Valid() bool {
	return s.it.Valid()
}

func (s AppDBIterator) Next() {
	s.it.Next()
}

func (s AppDBIterator) Value() (value []byte) {
	return s.it.Value()
}

func (s AppDBIterator) Error() error {
	return s.it.Error()
}

func (s AppDBIterator) Close() {
	s.it.Close()
}

func (s AppDBIterator) Domain() (start []byte, end []byte) {
	st, end := s.it.Domain()
	return KeyFromStoreKey(st), KeyFromStoreKey(end)
}
