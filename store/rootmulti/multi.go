package rootmulti

import (
	"fmt"
	"github.com/pokt-network/pocket-core/codec"
	reg "github.com/pokt-network/pocket-core/codec/types"
	"github.com/pokt-network/pocket-core/store/cachemulti"
	"github.com/pokt-network/pocket-core/store/types"
	sdk "github.com/pokt-network/pocket-core/types"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tendermint/tendermint/crypto/merkle"
	"github.com/tendermint/tendermint/crypto/tmhash"
	dbm "github.com/tendermint/tm-db"
	"io"
	"os"
	"path/filepath"
)

var _ types.CommitMultiStore = (*MultiStore)(nil)

const stateDBFolder = "app-state"

type MultiStore struct {
	AppDB        dbm.DB                               // storing data latestHeight -> latestHeight-pruneDepth
	stateDir     string                               // path to datadir
	lastCommitID types.CommitID                       // lastCommitID from the IAVL
	stores       map[types.StoreKey]types.CommitStore // the stores where all data besides appHash is queried
	pruneDepth   int                                  // depth where we will prune AppDB
}

func NewMultiStore(appDB dbm.DB, datadir string, pruneDepth int) *MultiStore {
	return &MultiStore{
		AppDB:      appDB,
		stateDir:   datadir + string(filepath.Separator) + stateDBFolder,
		stores:     make(map[types.StoreKey]types.CommitStore),
		pruneDepth: pruneDepth,
	}
}

func (rs *MultiStore) CopyStore() *types.Store {
	newStores := make(map[types.StoreKey]types.CommitStore)
	for k, v := range rs.stores {
		newStores[k] = v
	}
	s := types.Store(&MultiStore{
		AppDB:        rs.AppDB,
		lastCommitID: rs.lastCommitID,
		stores:       newStores,
		pruneDepth:   rs.pruneDepth,
	})
	return &s
}

func (rs *MultiStore) GetStoreType() types.StoreType {
	return types.StoreTypeMulti
}

func (rs *MultiStore) LoadLatestVersion() error {
	// nuke the statedb
	err := os.RemoveAll(rs.stateDir)
	if err != nil {
		fmt.Println("Unable to delete app-state folder @")
	}
	latest := getLatestVersion(rs.AppDB)
	return rs.LoadVersion(latest)
}

func (rs *MultiStore) LoadVersion(ver int64) error {
	if getLatestVersion(rs.AppDB) == ver {
		var newStores = make(map[types.StoreKey]types.CommitStore)
		if ver == 0 {
			for key := range rs.stores {
				store := NewStore(rs.AppDB, ver, key.Name(), types.CommitID{}, rs.stateDir, true)
				newStores[key] = store
			}
			rs.stores = newStores
			return nil
		}
		cInfo, err := getCommitInfo(rs.AppDB, ver)
		if err != nil {
			return err
		}
		// convert StoreInfos slice to map
		infos := make(map[types.StoreKey]StoreInfo)
		for _, storeInfo := range cInfo.StoreInfos {
			infos[rs.nameToKey(storeInfo.Name)] = storeInfo
		}
		for key := range rs.stores {
			var id types.CommitID
			info, ok := infos[key]
			if ok {
				id = info.Core.CommitID
			}
			store := NewStore(rs.AppDB, ver, key.Name(), id, rs.stateDir, true)
			newStores[key] = store
		}
		rs.lastCommitID = cInfo.CommitID()
		rs.stores = newStores
		return nil
	}
	panic("LoadVersion called for non-LatestHeight")
}

func (rs *MultiStore) LoadLazyVersion(ver int64) (*types.Store, error) {
	latestHeight := rs.lastCommitID.Version
	if rs.lastCommitID.Version == ver {
		return rs.CopyStore(), nil
	}
	if rs.Pruning() {
		oldestHeight := latestHeight - int64(rs.pruneDepth)
		if ver < oldestHeight {
			return nil, fmt.Errorf("unable to get version %d heights before %d are pruned", ver, oldestHeight)
		}
	}
	newStores := make(map[types.StoreKey]types.CommitStore)
	for k, v := range rs.stores {
		a, ok := (v).(*Store)
		if !ok {
			continue
		}
		newStores[k] = a.LoadImmutableVersion(ver, rs.stateDir)
	}
	s := types.Store(&MultiStore{
		AppDB:        rs.AppDB,
		lastCommitID: rs.lastCommitID,
		stores:       newStores,
		pruneDepth:   rs.pruneDepth,
	})
	return &s, nil
}

func (rs *MultiStore) Commit() types.CommitID {
	// Commit stores.
	commitInfo := CommitInfo{}
	batch := rs.AppDB.NewBatch()
	defer batch.Close()
	version := rs.lastCommitID.Version + 1
	commitInfo, batch = commitStores(version, int64(rs.pruneDepth), rs.stores, batch)
	setCommitInfo(batch, version, commitInfo)
	setLatestVersion(batch, version)
	_ = batch.Write()
	// Prepare for next version.
	commitID := types.CommitID{
		Version: version,
		Hash:    commitInfo.Hash(),
	}
	rs.lastCommitID = commitID
	return commitID
}

// Commits each store and returns a new commitInfo.
func commitStores(version, pruneAfter int64, storeMap map[types.StoreKey]types.CommitStore, b dbm.Batch) (ci CommitInfo, batch dbm.Batch) {
	storeInfos := make([]StoreInfo, 0, len(storeMap))
	// Prune
	pruneHeight := version - pruneAfter
	for key, store := range storeMap {
		s, ok := store.(*Store)
		if !ok {
			panic("non-Store mounted in commitStores")
		}
		// Commit
		commitID, batch := s.CommitBatch(b)
		// Record CommitID
		si := StoreInfo{}
		si.Name = key.Name()
		si.Core.CommitID = commitID
		// si.Core.StoreType = store.GetStoreType()
		storeInfos = append(storeInfos, si)

		if pruneAfter != -1 && pruneHeight > 0 {
			b = s.PruneVersion(batch, pruneHeight)
		}
	}
	ci = CommitInfo{
		Version:    version,
		StoreInfos: storeInfos,
	}
	return ci, b
}

func (rs *MultiStore) LastCommitID() types.CommitID {
	return rs.lastCommitID
}

func (rs *MultiStore) CacheWrap() types.CacheWrap {
	return rs.CacheMultiStore().(types.CacheWrap)
}

func (rs *MultiStore) CacheMultiStore() types.CacheMultiStore {
	return cachemulti.NewCacheMulti(rs.stores)
}

func (rs *MultiStore) GetStore(key types.StoreKey) types.Store {
	return rs.stores[key]
}

func (rs *MultiStore) GetKVStore(key types.StoreKey) types.KVStore {
	return rs.stores[key].(types.KVStore)
}

func (rs *MultiStore) MountStoreWithDB(key types.StoreKey, typ types.StoreType, db dbm.DB) {
	if typ == types.StoreTypeIAVL {
		rs.stores[key] = nil
	}
}

func (rs *MultiStore) GetCommitStore(key types.StoreKey) types.CommitStore {
	return rs.stores[key]
}

func (rs *MultiStore) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	return rs.GetCommitStore(key).(types.CommitKVStore)
}

func (rs *MultiStore) nameToKey(name string) types.StoreKey {
	for key := range rs.stores {
		if key.Name() == name {
			return key
		}
	}
	panic("Unknown name " + name)
}

func (rs *MultiStore) Pruning() bool {
	if rs.pruneDepth == -1 {
		return false
	}
	if rs.lastCommitID.Version-int64(rs.pruneDepth) < 0 {
		return false
	}
	return true
}

var cdc = codec.NewCodec(reg.NewInterfaceRegistry())

const (
	latestVersionKey = "s/latest"
	commitInfoKeyFmt = "s/%d" // s/<version>
)

func getLatestVersion(db dbm.DB) int64 {
	var latest sdk.Int64
	latestBytes, _ := db.Get([]byte(latestVersionKey))
	if latestBytes == nil {
		return 0
	}
	err := cdc.LegacyUnmarshalBinaryLengthPrefixed(latestBytes, &latest)
	if err != nil {
		panic(err)
	}

	return int64(latest)
}

// Gets commitInfo from disk.
func getCommitInfo(db dbm.DB, ver int64) (CommitInfo, error) {

	// Get from DB.
	cInfoKey := fmt.Sprintf(commitInfoKeyFmt, ver)
	cInfoBytes, _ := db.Get([]byte(cInfoKey))
	if cInfoBytes == nil {
		return CommitInfo{}, fmt.Errorf("failed to get Store: no data")
	}

	var cInfo CommitInfo

	err := cdc.LegacyUnmarshalBinaryLengthPrefixed(cInfoBytes, &cInfo)
	if err != nil {
		return CommitInfo{}, fmt.Errorf("failed to get Store: %v", err)
	}

	return cInfo, nil
}

// Hash returns the simple merkle root hash of the stores sorted by name.
func (ci *CommitInfo) Hash() []byte {
	// TODO: cache to ci.hash []byte
	m := make(map[string][]byte, len(ci.StoreInfos))
	for _, storeInfo := range ci.StoreInfos {
		m[storeInfo.Name] = storeInfo.Hash()
	}

	return merkle.SimpleHashFromMap(m)
}

func (ci *CommitInfo) CommitID() types.CommitID {
	return types.CommitID{
		Version: ci.Version,
		Hash:    ci.Hash(),
	}
}

// Implements merkle.Hasher.
func (si StoreInfo) Hash() []byte {
	// Doesn't write Name, since merkle.SimpleHashFromMap() will
	// include them via the keys.
	bz := si.Core.CommitID.Hash
	hasher := tmhash.New()

	_, err := hasher.Write(bz)
	if err != nil {
		// TODO: Handle with #870
		panic(err)
	}

	return hasher.Sum(nil)
}

// Set a commitInfo for given version.
func setCommitInfo(batch dbm.Batch, version int64, cInfo CommitInfo) {
	cInfoBytes, err := cdc.LegacyMarshalBinaryLengthPrefixed(&cInfo)
	if err != nil {
		panic(err)
	}
	cInfoKey := fmt.Sprintf(commitInfoKeyFmt, version)
	batch.Set([]byte(cInfoKey), cInfoBytes)
}

// Set the latest version.
func setLatestVersion(batch dbm.Batch, version int64) {
	v := sdk.Int64(version)
	latestBytes, _ := cdc.LegacyMarshalBinaryLengthPrefixed(&v)
	batch.Set([]byte(latestVersionKey), latestBytes)
}

func triggerCompaction(pruneHeight int64, appDB dbm.DB) {
	// manuall trigger compaction
	if pruneHeight%100 == 0 && pruneHeight != 0 {
		fmt.Println("triggering compaction")
		err := appDB.(*dbm.GoLevelDB).Compact(util.Range{})
		if err != nil {
			panic("unable to manually trigger compaction in commitStores")
		}
	}
}

// Deprecated below

func (rs *MultiStore) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	panic("CacheWrapWithTrace not implemented in MultiStore")
}

func (rs *MultiStore) SetPruning(types.Deprecated) {
	panic("SetPruning is deprecated in MultiStore")
}

func (rs *MultiStore) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	panic("CacheMultiStoreWithVersion is not implemented for MultiStore")
}

func (rs *MultiStore) TracingEnabled() bool {
	panic("Tracing is not implemented for MultiStore")
}

func (rs *MultiStore) SetTracer(w io.Writer) types.MultiStore {
	panic("Tracing is not implemented for MultiStore")
}

func (rs *MultiStore) SetTracingContext(types.TraceContext) types.MultiStore {
	panic("Tracing is not implemented for MultiStore")
}

func (rs *MultiStore) RollbackVersion(ver int64) error {
	panic("Rollback is not implemented for MultiStore")
}
