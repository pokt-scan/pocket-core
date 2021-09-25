package rootmulti

import (
	"fmt"
	"github.com/pokt-network/pocket-core/store/cachemulti"
	"github.com/pokt-network/pocket-core/store/iavl"
	"github.com/pokt-network/pocket-core/store/types"
	dbm "github.com/tendermint/tm-db"
	"io"
)

type Store struct {
	appDB     dbm.DB      // parent applicationDB (finality)
	state     dbm.DB      // ephemeral state for current height (only on latest height)
	iavl      *iavl.Store // ephemeral iavl for merkle tree (only on latest height)
	storeKey  string      // key of store
	height    int64       // height of store
	isMutable bool        // whether or not to use appDB or stateDB
}

// NewStore creates a new 'store' object.
// If mutable is set true, it loads the appDB data into state and iavl (NOTE: this should only happen on first launch)
func NewStore(appDB dbm.DB, height int64, storeKey string, commitID types.CommitID, stateDir string, isMutable bool) *Store {
	// mutable, ephemeral stores that get written to appDB
	var state dbm.DB
	var iavlStore *iavl.Store
	// if we are operating on a mutable height (latest height)
	if isMutable {
		storeHeightKey := StoreKey(height-1, storeKey, "")
		it, err := appDB.Iterator(storeHeightKey, types.PrefixEndBytes(storeHeightKey))
		if err != nil {
			panic(fmt.Sprintf("unable to create an iterator for height %d storeKey %s", height, storeKey))
		}
		defer it.Close()
		// load the state into mem
		state, err = dbm.NewGoLevelDB(storeKey, stateDir)
		if err != nil {
			panic(err)
		}
		for ; it.Valid(); it.Next() {
			err := state.Set(KeyFromStoreKey(it.Key()), it.Value())
			if err != nil {
				panic("unable to set k/v in memDB in NewStore: " + err.Error())
			}
		}
		// create load iavl store
		cs, err := iavl.LoadStore(dbm.NewPrefixDB(appDB, []byte("s/k:"+storeKey+"/")), commitID, types.Deprecated{}, false)
		if err != nil {
			panic("unable to load iavlStore in rootmultistore: " + err.Error())
		}
		iavlStore = cs.(*iavl.Store)
	}
	return &Store{
		appDB:     appDB,
		state:     state,     // state is nil if is immutable
		iavl:      iavlStore, // iavl is nil if is immutable
		storeKey:  storeKey,
		isMutable: isMutable, // aka 'onLatestHeight?'
		height:    height,
	}
}

// Loads an older height from the latest
func (is *Store) LoadImmutableVersion(version int64, stateDir string) *Store {
	return NewStore(is.appDB, version, is.storeKey, types.CommitID{}, stateDir, false)
}

func (is *Store) Get(key []byte) ([]byte, error) {
	if is.isMutable {
		return is.state.Get(key) // if on the latest height use state (memoryLevelDB) to query
	}
	return is.appDB.Get(StoreKey(is.height-1, is.storeKey, string(key)))
}

func (is *Store) Has(key []byte) (bool, error) {
	if is.isMutable {
		return is.state.Has(key) // if on the latest height use state (memoryLevelDB) to query
	}
	return is.appDB.Has(StoreKey(is.height-1, is.storeKey, string(key)))
}

func (is *Store) Set(key, value []byte) error {
	if is.isMutable {
		err := is.iavl.Set(key, value)
		if err != nil {
			panic("unable to set to iavl: " + err.Error())
		}
		return is.state.Set(key, value)
	}
	panic("'Set()' called on immutable store")
}

func (is *Store) Delete(key []byte) error {
	if is.isMutable {
		err := is.iavl.Delete(key)
		if err != nil {
			panic("unable to delete to iavl: " + err.Error())
		}
		return is.state.Delete(key)
	}
	panic("'Delete()' called on immutable store")
}

func (is *Store) Iterator(start, end []byte) (types.Iterator, error) {
	if is.isMutable {
		return is.state.Iterator(start, end)
	}
	baseIterator, err := is.appDB.Iterator(StoreKey(is.height-1, is.storeKey, string(start)), StoreKey(is.height-1, is.storeKey, string(end)))
	return AppDBIterator{it: baseIterator}, err
}

func (is *Store) ReverseIterator(start, end []byte) (types.Iterator, error) {
	if is.isMutable {
		return is.state.ReverseIterator(start, end)
	}
	baseIterator, err := is.appDB.ReverseIterator(StoreKey(is.height-1, is.storeKey, string(start)), StoreKey(is.height-1, is.storeKey, string(end)))
	return AppDBIterator{it: baseIterator}, err
}

// Commit the iavl and the state into persistence
func (is *Store) CommitBatch(b dbm.Batch) (commitID types.CommitID, batch dbm.Batch) {
	if b == nil {
		b = is.appDB.NewBatch()
	}
	commitID = is.iavl.Commit() // TODO IAVL operations are not atomic :(
	// iterate through the local state and commit that to the appDB in a batch
	it, err := is.state.Iterator(nil, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to create an iterator for height %d storeKey %s in Commit()", is.height, is.storeKey))
	}
	defer it.Close()
	// load the state into mem
	for ; it.Valid(); it.Next() {
		b.Set(StoreKey(is.height, is.storeKey, string(it.Key())), it.Value())
	}
	is.height += 1
	return commitID, b
}

func (is *Store) Commit() types.CommitID {
	// TODO IAVL operations are not atomic :(
	// go ahead and commit the iavl store
	commitID := is.iavl.Commit()
	// iterate through the local state and commit that to the appDB in a batch
	it, err := is.state.Iterator(nil, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to create an iterator for height %d storeKey %s in Commit()", is.height, is.storeKey))
	}
	defer it.Close()
	// load the state into mem
	for ; it.Valid(); it.Next() {
		err := is.appDB.Set(StoreKey(is.height, is.storeKey, string(it.Key())), it.Value())
		if err != nil {
			panic(err)
		}
	}
	is.height += 1
	return commitID
}

// Prune a version from both the iavl and the appDB
func (is *Store) PruneVersion(batch dbm.Batch, version int64) dbm.Batch {
	if batch == nil {
		batch = is.appDB.NewBatch()
	}
	is.iavl.DeleteVersion(version) // TODO IAVL operations are not atomic :(
	prefixKey := StoreKey(version, is.storeKey, "")
	it, err := is.appDB.Iterator(prefixKey, types.PrefixEndBytes(prefixKey))
	if err != nil {
		panic("unable to create iterator in PruneVersion for appDB")
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		batch.Delete(it.Key())
	}
	return batch
}

func (is *Store) CacheWrap() types.CacheWrap {
	return cachemulti.NewStoreCache(is)
}

func (is *Store) LastCommitID() types.CommitID {
	if is.isMutable {
		return is.iavl.LastCommitID()
	}
	panic("LastCommitID for called on an immutable store")
}

func (is *Store) GetStoreType() types.StoreType {
	return types.StoreTypeIAVL
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

// An iterator that is 'height/store.Name()' aware
type AppDBIterator struct {
	it dbm.Iterator
}

func (s AppDBIterator) Domain() (start []byte, end []byte) {
	panic("Domain() not implemented for storeIterator")
}

func (s AppDBIterator) Valid() bool {
	return s.it.Valid()
}

func (s AppDBIterator) Next() {
	s.it.Next()
}

func (s AppDBIterator) Key() (key []byte) {
	return KeyFromStoreKey(s.it.Key())
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

// deprecated below

func (is *Store) SetPruning(types.Deprecated) {
	panic("SetPruning is deprecated for Store")
}

func (is *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	panic("CacheWrapWithTrace for Store not implemented")
}
