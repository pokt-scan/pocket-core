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
	"os"
	"path/filepath"
)

var _ types.CommitMultiStore = (*MultiStore)(nil)

const stateDBFolder = "app-state"

type MultiStore struct {
	AppDB        dbm.DB                               // application db, contains everything but the state
	stateDir     string                               // path to statedb folder
	stores       map[types.StoreKey]types.CommitStore // prefixed abstractions; living inside appDB
	lastCommitID types.CommitID                       // lastCommitID from the IAVL
	pruneDepth   int64                                // -1 is off
}

func NewMultiStore(appDB dbm.DB, datadir string, pruneDepth int64) *MultiStore {
	return &MultiStore{
		AppDB:      appDB,
		stateDir:   datadir + string(filepath.Separator) + stateDBFolder,
		stores:     make(map[types.StoreKey]types.CommitStore),
		pruneDepth: pruneDepth,
	}
}

// read or write
func (rs *MultiStore) LoadLatestVersion() error {
	// nuke the state
	_ = os.RemoveAll(rs.stateDir)
	// get latest height
	latestHeight := getLatestVersion(rs.AppDB)
	// if genesis
	if latestHeight == 0 {
		for key := range rs.stores {
			store := NewStore(rs.AppDB, latestHeight, key.Name(), types.CommitID{}, rs.stateDir, true)
			rs.stores[key] = store
		}
		return nil
	}
	// get commit information
	cInfo, err := getCommitInfo(rs.AppDB, latestHeight)
	if err != nil {
		return err
	}
	rs.lastCommitID = cInfo.CommitID()
	// convert slice into map
	infos := make(map[string]StoreInfo)
	for _, storeInfo := range cInfo.StoreInfos {
		infos[storeInfo.Name] = storeInfo
	}
	// create new mutable store
	for key := range rs.stores {
		rs.stores[key] = NewStore(rs.AppDB, latestHeight, key.Name(), infos[key.Name()].Core.CommitID, rs.stateDir, true)
	}
	return nil
}

// read only
func (rs *MultiStore) LoadImmutableVersion(height int64) (*types.Store, error) {
	// if latest height
	if rs.lastCommitID.Version == height {
		return rs.CopyStore(), nil
	}
	// ensure not pruned
	if rs.Pruning() {
		oldestHeight := rs.lastCommitID.Version - int64(rs.pruneDepth)
		if height < oldestHeight {
			return nil, fmt.Errorf("unable to get version %d heights before %d are pruned", height, oldestHeight)
		}
	}
	// load immutable from previous stores
	prevStores := make(map[types.StoreKey]types.CommitStore)
	for k, store := range rs.stores {
		prevStores[k] = store.(*Store).LoadImmutableVersion(height, rs.stateDir)
	}
	// create struct & return
	ms := types.Store(&MultiStore{
		AppDB:        rs.AppDB,
		lastCommitID: rs.lastCommitID,
		stores:       prevStores,
		pruneDepth:   rs.pruneDepth,
	})
	return &ms, nil
}

// Persist IAVL & StateDB
func (rs *MultiStore) Commit() types.CommitID {
	// create atomic batch
	batch := rs.AppDB.NewBatch()
	defer batch.Close()
	// increment height
	height := rs.lastCommitID.Version + 1
	// create new commitInfo
	commitInfo := CommitInfo{
		Version:    height,
		StoreInfos: make([]StoreInfo, 0, len(rs.stores)),
	}
	// for each store; CommitBatch() & add CommitID to CommitInfo
	// if Pruning(); prune height - depth
	for key, store := range rs.stores {
		commitID, batch := store.(*Store).CommitBatch(batch)
		commitInfo.StoreInfos = append(commitInfo.StoreInfos, StoreInfo{
			Name: key.Name(),
			Core: StoreCore{
				CommitID: commitID,
			},
		})
		if rs.Pruning() {
			batch = store.(*Store).PruneVersion(batch, height-rs.pruneDepth)
		}
	}
	// save commitInfo & latestHeight
	setCommitInfo(batch, height, commitInfo)
	setLatestVersion(batch, height)
	// write the batch
	_ = batch.Write()
	// prep next height
	rs.lastCommitID = types.CommitID{
		Version: height,
		Hash:    commitInfo.Hash(),
	}
	return rs.lastCommitID
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
	if typ == types.StoreTypeDefault {
		rs.stores[key] = nil
	}
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

// Get CommitInfo from disk
func getCommitInfo(db dbm.DB, ver int64) (CommitInfo, error) {
	// get from store
	cInfoBz, _ := db.Get([]byte(fmt.Sprintf(commitInfoKeyFmt, ver)))
	if cInfoBz == nil {
		return CommitInfo{}, fmt.Errorf("failed to get Store: no data")
	}
	// unmarshal from amino bytes
	var cInfo CommitInfo
	err := cdc.LegacyUnmarshalBinaryLengthPrefixed(cInfoBz, &cInfo)
	if err != nil {
		return CommitInfo{}, fmt.Errorf("failed to get Store: %v", err)
	}
	return cInfo, nil
}

// Hash returns the simple merkle root hash of the stores sorted by name
func (ci *CommitInfo) Hash() []byte {
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
	bz := si.Core.CommitID.Hash
	hasher := tmhash.New()
	_, err := hasher.Write(bz)
	if err != nil {
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

// manually trigger db compaction
func triggerCompaction(pruneHeight int64, appDB dbm.DB) {
	if pruneHeight%100 == 0 && pruneHeight != 0 {
		fmt.Println("triggering compaction")
		err := appDB.(*dbm.GoLevelDB).Compact(util.Range{})
		if err != nil {
			panic(err)
		}
	}
}
