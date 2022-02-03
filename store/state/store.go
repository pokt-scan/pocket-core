package state

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/pokt-network/pocket-core/store/cachemulti"
	"github.com/pokt-network/pocket-core/store/iavl"
	"github.com/pokt-network/pocket-core/store/sqlitedb"
	"github.com/pokt-network/pocket-core/store/types"
	dbm "github.com/tendermint/tm-db"
	"strconv"
	"strings"
)

var _ types.KVStore = (*Store)(nil)
var _ types.CommitStore = (*Store)(nil)
var _ types.CommitKVStore = (*Store)(nil)

// Prefixed abstractions living inside AppDB;
type Store struct {
	sqLiteDB              *sqlitedb.SQLiteDB // Inmutable app state database
	appDB                 dbm.DB             // parent db, (where everything lives except for state)
	state                 dbm.DB             // ephemeral state used to 'stage' potential changes; only latest height; nuked on startup;
	iavl                  *iavl.Store        // used for latest height state commitments ONLY; may be pruned;
	storeKey              string             // constant; part of the prefix
	height                int64              // dynamic; part of the prefix
	isMutable             bool               // !isReadOnly
	immutableLatestHeight int64              // READ ONLY IMMUTABLE LATEST KNOWN HEIGHT
	debug                 bool
	hasCommitted          bool
	pruneOption           types.PruningOptions
	logOperation          bool
	mockPruning           bool
}

func NewStore(appDB dbm.DB, height int64, storeKey string, commitID types.CommitID, stateDir string, baseDatadir string, isMutable bool, immutableLatestHeight int64, pruneOption types.PruningOptions) *Store {
	store := &Store{
		appDB:                 appDB,
		storeKey:              storeKey,
		isMutable:             isMutable,
		height:                height,
		debug:                 false,
		logOperation:          true,
		mockPruning:           true,
		immutableLatestHeight: immutableLatestHeight,
		pruneOption:           pruneOption,
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
		// TODO: Separate these db
		// load IAVL from AppDB
		store.iavl, err = iavl.LoadStore(dbm.NewPrefixDB(appDB, []byte("s/k:"+storeKey+"/")), commitID, types.PruneNothing, false)
		if err != nil {
			panic("unable to load iavlStore in rootmultistore: " + err.Error())
		}
	}

	// Load the app state db for this store
	asdb, asdbErr := sqlitedb.NewAppStateDB(baseDatadir, storeKey)
	if asdbErr != nil {
		panic("Unable to load app statedb in store: " + asdbErr.Error())
	}
	store.sqLiteDB = asdb

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
	//fmt.Println(fmt.Sprintf("Store height: %d", is.height))
	//fmt.Println(fmt.Sprintf("Latest height: %d", is.getLatestHeight()))
	//fmt.Println(fmt.Sprintf("Prune option keep recent: %d", is.pruneOption.KeepRecent()))
	result := is.height < is.getLatestHeight()-is.pruneOption.KeepRecent()
	//fmt.Println(fmt.Sprintf("Result: %t", result))
	//if result == true {
	//	fmt.Println("It's true")
	//}
	return result
}

func (is *Store) LoadImmutableVersion(version int64, stateDir string, baseDatadir string, immutableLatestHeight int64, pruneOption types.PruningOptions) *Store {
	return NewStore(is.appDB, version, is.storeKey, types.CommitID{}, stateDir, baseDatadir, false, immutableLatestHeight, pruneOption)
}

func (is *Store) stateGet(key []byte) ([]byte, error) {
	return is.state.Get(key)
}

func (is *Store) appDBGet(key []byte) ([]byte, error) {
	return is.appDB.Get(StoreKey(is.height-1, is.storeKey, string(key)))
}

func (is *Store) Get(key []byte) (result []byte, err error) {
	if is.isMutable {
		result, err = is.stateGet(key)
	} else {
		if is.isHistoricalQuery() {
			// If we're logging operations and mocking pruning
			if is.logOperation && is.mockPruning {
				result, err = is.appDBGet(key)
				if err != nil {
					fmt.Println(err)
				}
				opRecord := []string{"##GET", is.storeKey, strconv.FormatInt(is.height-1, 10), bytesToBase64(key), bytesToBase64(result)}
				logOperation(opRecord)
			} else {
				// We're just doing a plain old historical query
				result, err = is.sqLiteDB.GetMutable(is.height-1, is.storeKey, key)
			}
		} else {
			// Get the historical data from the unpruned appdb
			result, err = is.appDBGet(key)
		}
	}

	// TODO refactor this
	if is.debug {
		var height = is.height
		if !is.isMutable {
			height = height - 1
		}

		result, resultErr := is.sqLiteDB.GetMutable(is.height-1, is.storeKey, key)
		if resultErr != nil {
			panic("Error on sqLiteDB get: " + resultErr.Error())
		}

		appDBGet, appDBGetErr := is.appDBGet(key)
		if appDBGetErr != nil {
			panic("Error on appdb get: " + appDBGetErr.Error())
		}

		if !bytes.Equal(appDBGet, result) || (appDBGet == nil && result != nil) || (appDBGet != nil && result == nil) {
			fmt.Println(fmt.Sprintf("Different get results for key: %s", hex.EncodeToString(key)))
			fmt.Println(fmt.Sprintf("IAVL Value: %s", hex.EncodeToString(appDBGet)))
			fmt.Println(fmt.Sprintf("ASDB Value: %s", hex.EncodeToString(result)))
			panic(fmt.Sprintf("Difference in get response between iavl get %s and result get %s", appDBGet, result))
		}

		return result, resultErr
	}

	return result, err
}

// Is never called
func (is *Store) Has(key []byte) (bool, error) {
	if is.isMutable { // if latestHeight
		return is.state.Has(key)
	}

	if is.isHistoricalQuery() {
		return is.sqLiteDB.HasMutable(is.height-1, is.storeKey, key)
	} else {
		return is.appDB.Has(StoreKey(is.height-1, is.storeKey, string(key)))
	}
}

func (is *Store) iavlSet(key, value []byte) error {
	err := is.iavl.Set(key, value)
	if err != nil {
		panic("unable to set to iavl: " + err.Error())
	}
	return err
}

func (is *Store) stateSet(key, value []byte) error {
	err := is.state.Set(key, value)
	if err != nil {
		panic("unable to set to state: " + err.Error())
	}
	return err
}

func (is *Store) Set(key, value []byte) (err error) {
	if is.isMutable {
		// Set the Iavl
		is.iavlSet(key, value)

		// Set the state db
		is.stateSet(key, value)

		// Historical data set
		asdbErr := is.sqLiteDB.SetMutable(is.height, is.storeKey, key, value)
		if asdbErr != nil {
			panic("unable to set to sqLiteDB: " + asdbErr.Error())
		}

		if is.logOperation && is.mockPruning {
			opRecord := []string{"##SET", is.storeKey, strconv.FormatInt(is.height, 10), bytesToBase64(key), bytesToBase64(value)}
			logOperation(opRecord)
		}

		return asdbErr
	}
	panic("'Set()' called on immutable store")
}

func (is *Store) iavlDelete(key []byte) error {
	err := is.iavl.Delete(key)
	if err != nil {
		panic("unable to delete to iavl: " + err.Error())
	}
	return err
}

func (is *Store) stateDelete(key []byte) error {
	err := is.state.Delete(key)
	if err != nil {
		panic("unable to delete to state: " + err.Error())
	}
	return err
}

func (is *Store) Delete(key []byte) (err error) {
	if is.isMutable {
		// Iavl delete
		is.iavlDelete(key)

		// State delete
		is.stateDelete(key)

		// Historical delete
		asdbErr := is.sqLiteDB.DeleteMutable(is.height, is.storeKey, key)
		if asdbErr != nil {
			panic("unable to set to sqLiteDB: " + asdbErr.Error())
		}

		if is.logOperation && is.mockPruning {
			opRecord := []string{"##DEL", is.storeKey, strconv.FormatInt(is.height, 10), bytesToBase64(key)}
			logOperation(opRecord)
		}

		return asdbErr
	}
	panic("'Delete()' called on immutable store")
}

func iteratorOutputHash(iterator types.Iterator) string {
	var result []string
	for iterator.Valid() {
		keyStr := bytesToBase64(iterator.Key())
		valStr := bytesToBase64(iterator.Value())
		entry := []string{keyStr, valStr}
		result = append(result, strings.Join(entry, ":"))
		iterator.Next()
	}
	return strings.Join(result, ";")
}

func iteratorEquals(iterator1, iterator2 types.Iterator) bool {
	// First compares validity
	if iterator1.Valid() != iterator2.Valid() {
		return false
	}

	// Compare contents and order
	for iterator1.Valid() && iterator2.Valid() {
		if !bytes.Equal(iterator1.Key(), iterator2.Key()) || !bytes.Equal(iterator1.Value(), iterator2.Value()) {
			fmt.Println(fmt.Sprintf("Iterator Keys are different between %s and %s", hex.EncodeToString(iterator1.Key()), hex.EncodeToString(iterator2.Key())))
			fmt.Println(fmt.Sprintf("Iterator Values are different between %s and %s", hex.EncodeToString(iterator1.Value()), hex.EncodeToString(iterator2.Value())))
			return false
		}

		if (iterator1.Key() == nil && iterator2.Key() != nil) || (iterator2.Key() == nil && iterator1.Key() != nil) {
			fmt.Println(fmt.Sprintf("Iterator Keys are different between %s and %s", hex.EncodeToString(iterator1.Key()), hex.EncodeToString(iterator2.Key())))
			fmt.Println(fmt.Sprintf("Iterator Values are different between %s and %s", hex.EncodeToString(iterator1.Value()), hex.EncodeToString(iterator2.Value())))
			return false
		}

		if (iterator1.Value() == nil && iterator2.Value() != nil) || (iterator2.Value() == nil && iterator1.Value() != nil) {
			fmt.Println(fmt.Sprintf("Iterator Keys are different between %s and %s", hex.EncodeToString(iterator1.Key()), hex.EncodeToString(iterator2.Key())))
			fmt.Println(fmt.Sprintf("Iterator Values are different between %s and %s", hex.EncodeToString(iterator1.Value()), hex.EncodeToString(iterator2.Value())))
			return false
		}
		iterator1.Next()
		iterator2.Next()

		if iterator1.Valid() != iterator2.Valid() {
			if iterator1.Valid() {
				fmt.Println("PRINTING THE REMAINDER OF ITERATOR 1 ENTRIES")
				for iterator1.Valid() {
					fmt.Println("------------")
					fmt.Println(fmt.Sprintf("Key: %s", hex.EncodeToString(iterator1.Key())))
					fmt.Println(fmt.Sprintf("Value: %s", hex.EncodeToString(iterator1.Value())))
					fmt.Println("------------")
					iterator1.Next()
				}
			}

			if iterator2.Valid() {
				fmt.Println("PRINTING THE REMAINDER OF ITERATOR 2 ENTRIES")
				for iterator2.Valid() {
					fmt.Println("------------")
					fmt.Println(fmt.Sprintf("Key: %s", hex.EncodeToString(iterator2.Key())))
					fmt.Println(fmt.Sprintf("Value: %s", hex.EncodeToString(iterator2.Value())))
					fmt.Println("------------")
					iterator2.Next()
				}
			}
			return false
		}
	}
	return true
}

func (is *Store) stateIterator(start, end []byte) (types.Iterator, error) {
	return is.state.Iterator(start, end)
}

func (is *Store) appDBIterator(start, end []byte) (types.Iterator, error) {
	baseIterator, err := is.appDB.Iterator(StoreKey(is.height-1, is.storeKey, string(start)), StoreKey(is.height-1, is.storeKey, string(end)))
	return AppDBIterator{it: baseIterator}, err
}

func (is *Store) Iterator(start, end []byte) (it types.Iterator, err error) {
	if is.isMutable {
		it, err = is.stateIterator(start, end)
	} else {
		if is.isHistoricalQuery() {
			// If we're logging operations and mocking pruning
			if is.logOperation && is.mockPruning {
				it, err = is.appDBIterator(start, end)
				if err != nil {
					fmt.Println(err)
				}
				opRecord := []string{"##ITE", is.storeKey, strconv.FormatInt(is.height-1, 10), bytesToBase64(start), bytesToBase64(end), iteratorOutputHash(it)}
				logOperation(opRecord)

				it, err = is.appDBIterator(start, end)
			} else {
				// We're just doing a plain old historical query
				it, err = is.sqLiteDB.IteratorMutable(is.height-1, is.storeKey, start, end)
			}
		} else {
			it, err = is.appDBIterator(start, end)
		}
	}

	if is.debug {
		var height = is.height
		if !is.isMutable {
			height = height - 1
		}
		//asdbItStmt := is.sqLiteDB.IteratorMutableStmt(height, is.storeKey, start, end)
		//expectedValueHash := iteratorOutputHash(it)
		//logDebug(is.height, "IT", is.storeKey, asdbItStmt, expectedValueHash)
		appDBIterator, appDBItErr := is.appDBIterator(start, end)
		if appDBItErr != nil {
			panic("Error on appdb iterator: " + appDBItErr.Error())
		}

		asdbIt, asdbItErr := is.sqLiteDB.IteratorMutable(is.height-1, is.storeKey, start, end)
		if asdbItErr != nil {
			panic("Error on sqLiteDB iterator: " + asdbItErr.Error())
		}

		if !iteratorEquals(appDBIterator, asdbIt) {
			fmt.Println(fmt.Sprintf("Different Iterators on height: %d", is.height))
			fmt.Println(fmt.Sprintf("Different Iterators with start: %s and end: %s", hex.EncodeToString(start), hex.EncodeToString(end)))
			panic(fmt.Sprintf("Different Iterators on table %s", is.storeKey))
		}

		return asdbIt, asdbItErr
		if is.isMutable {
			it, err = is.stateIterator(start, end)
		} else {
			if is.isHistoricalQuery() {
				it, err = is.sqLiteDB.IteratorMutable(is.height-1, is.storeKey, start, end)
			} else {
				it, err = is.appDBIterator(start, end)
			}
		}
	}

	return it, err
}

func (is *Store) stateReverseIterator(start, end []byte) (types.Iterator, error) {
	//defer store.TimeTrack(time.Now(), fmt.Sprintf("State Reverse Iterator with order DESC"))
	//return is.state.ReverseIterator(start, end)
	return is.iavl.ReverseIterator(start, end)
}

func (is *Store) appDBReverseIterator(start, end []byte) (types.Iterator, error) {
	//defer store.TimeTrack(time.Now(), fmt.Sprintf("AppDB Reverse Iterator with order DESC"))
	baseIterator, err := is.appDB.ReverseIterator(StoreKey(is.height-1, is.storeKey, string(start)), StoreKey(is.height-1, is.storeKey, string(end)))
	return AppDBIterator{it: baseIterator}, err
}

func (is *Store) ReverseIterator(start, end []byte) (it types.Iterator, err error) {
	if is.isMutable {
		it, err = is.stateReverseIterator(start, end)
		//return is.stateReverseIterator(start, end)
	} else {
		if is.isHistoricalQuery() {
			// If we're logging operations and mocking pruning
			if is.logOperation && is.mockPruning {
				it, err = is.appDBReverseIterator(start, end)
				if err != nil {
					fmt.Println(err)
				}
				opRecord := []string{"##RIT", is.storeKey, strconv.FormatInt(is.height-1, 10), bytesToBase64(start), bytesToBase64(end), iteratorOutputHash(it)}
				logOperation(opRecord)

				it, err = is.appDBReverseIterator(start, end)
			} else {
				// We're just doing a plain old historical query
				it, err = is.sqLiteDB.ReverseIteratorMutable(is.height-1, is.storeKey, start, end)
			}
		} else {
			it, err = is.appDBReverseIterator(start, end)
		}
	}

	if is.debug {
		var height = is.height
		if !is.isMutable {
			height = height - 1
		}
		//asdbItStmt := is.sqLiteDB.ReverseIteratorMutableStmt(height, is.storeKey, start, end)
		//expectedValueHash := iteratorOutputHash(it)
		//logDebug(is.height, "IT", is.storeKey, asdbItStmt, expectedValueHash)
		appDBIterator, appDBItErr := is.appDBReverseIterator(start, end)
		if appDBItErr != nil {
			panic("Error on appdb iterator: " + appDBItErr.Error())
		}

		asdbIt, asdbItErr := is.sqLiteDB.ReverseIteratorMutable(is.height-1, is.storeKey, start, end)
		if asdbItErr != nil {
			panic("Error on sqLiteDB iterator: " + asdbItErr.Error())
		}

		if !iteratorEquals(appDBIterator, asdbIt) {
			fmt.Println(fmt.Sprintf("Different Iterators on height: %d", is.height))
			fmt.Println(fmt.Sprintf("Different Iterators with start: %s and end: %s", hex.EncodeToString(start), hex.EncodeToString(end)))
			panic(fmt.Sprintf("Different Iterators on table %s", is.storeKey))
		}

		if is.isMutable {
			it, err = is.stateReverseIterator(start, end)
		} else {
			if is.isHistoricalQuery() {
				it, err = is.sqLiteDB.ReverseIteratorMutable(is.height-1, is.storeKey, start, end)
			} else {
				it, err = is.appDBReverseIterator(start, end)
			}
		}
	}

	return it, err
}

func (is *Store) iavlCommit() (commitID types.CommitID) {
	//defer store.TimeTrack(time.Now(), fmt.Sprintf("IAVL CommitMutable"))
	return is.iavl.Commit()
}

func (is *Store) stateCommit(b dbm.Batch) (batch dbm.Batch) {
	it, err := is.state.Iterator(nil, nil)
	if err != nil {
		panic(fmt.Sprintf("unable to create an iterator for height %d storeKey %s in Commit()", is.height, is.storeKey))
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		b.Set(StoreKey(is.height, is.storeKey, string(it.Key())), it.Value())
	}
	return b
}

// Persist State & IAVL
func (is *Store) CommitBatch(b dbm.Batch) (commitID types.CommitID, batch dbm.Batch) {
	// commit iavl
	commitID = is.iavlCommit() //is.iavl.Commit()
	// commit entire state
	b = is.stateCommit(b)

	// Commit to the historical db (sqLiteDB)
	commitErr := is.sqLiteDB.CommitMutable()
	if commitErr != nil {
		panic(commitErr.Error())
	}
	is.hasCommitted = true
	is.height++
	return commitID, b
}

// Prune version in IAVL & AppDB
func (is *Store) PruneVersion(batch dbm.Batch, version int64) dbm.Batch {
	if is.mockPruning {
		return batch
	}
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

func logOperation(record []string) {
	fmt.Println(strings.Join(record, `,`))
}

func bytesToBase64(input []byte) string {
	bas64Key := make([]byte, base64.StdEncoding.EncodedLen(len(input)))
	base64.StdEncoding.Encode(bas64Key, input)
	return string(bas64Key)
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
