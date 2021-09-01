package app

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pokt-network/pocket-core/store/rootmulti"
	"github.com/syndtr/goleveldb/leveldb/util"
	log2 "github.com/tendermint/tendermint/libs/log"
	state2 "github.com/tendermint/tendermint/state"
	db2 "github.com/tendermint/tm-db"
	"log"
	"os"
	"reflect"

	"github.com/pokt-network/pocket-core/crypto"
	sdk "github.com/pokt-network/pocket-core/types"
	"github.com/pokt-network/pocket-core/x/auth"
	"github.com/pokt-network/pocket-core/x/auth/types"
	pocketKeeper "github.com/pokt-network/pocket-core/x/pocketcore/keeper"
)

func GenerateAAT(appPubKey, clientPubKey string, key crypto.PrivateKey) (aatjson []byte, err error) {
	aat, er := pocketKeeper.AATGeneration(appPubKey, clientPubKey, key)
	if er != nil {
		return nil, er
	}
	return json.MarshalIndent(aat, "", "  ")
}

func BuildMultisig(fromAddr, jsonMessage, passphrase, chainID string, pk crypto.PublicKeyMultiSig, fees int64, legacyCodec bool) ([]byte, error) {
	fa, err := sdk.AddressFromHex(fromAddr)
	if err != nil {
		return nil, err
	}
	var m sdk.Msg
	if err := Codec().UnmarshalJSON([]byte(jsonMessage), &m); err != nil {
		return nil, err
	}
	// use reflection to convert to proto msg
	val := reflect.ValueOf(m)
	vp := reflect.New(val.Type())
	vp.Elem().Set(val)
	protoMsg := vp.Interface().(sdk.ProtoMsg)
	kb, err := GetKeybase()
	if err != nil {
		return nil, err
	}
	txBuilder := auth.NewTxBuilder(
		auth.DefaultTxEncoder(cdc),
		auth.DefaultTxDecoder(cdc),
		chainID,
		"", nil).WithKeybase(kb)
	return txBuilder.BuildAndSignMultisigTransaction(fa, pk, protoMsg, passphrase, fees, legacyCodec)
}

func SignMultisigNext(fromAddr, txHex, passphrase, chainID string, legacyCodec bool) ([]byte, error) {
	fa, err := sdk.AddressFromHex(fromAddr)
	if err != nil {
		return nil, err
	}
	bz, err := hex.DecodeString(txHex)
	if err != nil {
		return nil, err
	}
	kb, err := GetKeybase()
	if err != nil {
		return nil, err
	}
	txBuilder := auth.NewTxBuilder(
		auth.DefaultTxEncoder(cdc),
		auth.DefaultTxDecoder(cdc),
		chainID,
		"", nil).WithKeybase(kb)
	return txBuilder.SignMultisigTransaction(fa, nil, passphrase, bz, legacyCodec)
}

func SignMultisigOutOfOrder(fromAddr, txHex, passphrase, chainID string, keys []crypto.PublicKey, legacyCodec bool) ([]byte, error) {
	fa, err := sdk.AddressFromHex(fromAddr)
	if err != nil {
		return nil, err
	}
	bz, err := hex.DecodeString(txHex)
	if err != nil {
		return nil, err
	}
	kb, err := GetKeybase()
	if err != nil {
		return nil, err
	}
	txBuilder := auth.NewTxBuilder(
		auth.DefaultTxEncoder(cdc),
		auth.DefaultTxDecoder(cdc),
		chainID,
		"", nil).WithKeybase(kb)
	return txBuilder.SignMultisigTransaction(fa, keys, passphrase, bz, legacyCodec)
}

func SortJSON(toSortJSON []byte) string {
	var c interface{}
	err := json.Unmarshal(toSortJSON, &c)
	if err != nil {
		log.Fatal("could not unmarshal json in SortJSON: " + err.Error())
	}
	js, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		log.Fatalf("could not marshal back to json in SortJSON: " + err.Error())
	}
	return string(js)
}

func UnmarshalTxStr(txStr string, height int64) types.StdTx {
	txBytes, err := base64.StdEncoding.DecodeString(txStr)
	if err != nil {
		log.Fatal("error:", err)
	}
	return UnmarshalTx(txBytes, height)
}

func UnmarshalTx(txBytes []byte, height int64) types.StdTx {
	defaultTxDecoder := auth.DefaultTxDecoder(cdc)
	tx, err := defaultTxDecoder(txBytes, height)
	if err != nil {
		log.Fatalf("Could not decode transaction: " + err.Error())
	}
	return tx.(auth.StdTx)
}

func UnsafeDeleteData(config sdk.Config, lastDeleteHeight int64) {
	// setup the database
	db, err := OpenApplicationDB(config)
	if err != nil {
		fmt.Println("error loading application database: ", err)
		return
	}
	blockStore, _, blockStoreDB, _, err := state2.BlocksAndStateFromDB(&config.TendermintConfig, state2.DefaultDBProvider)
	if err != nil {
		fmt.Println("error loading blockstore/state db: ", err)
		return
	}
	maxPruneHeight := blockStore.Height() - 100
	if maxPruneHeight < 0 {
		maxPruneHeight = 0
	}
	if lastDeleteHeight >= maxPruneHeight {
		fmt.Printf("Can't prune up to %d; You must maintain atleast 100 blocks for safety\nMaxPruneHeight is %d\n", lastDeleteHeight, maxPruneHeight)
	}
	fmt.Println("Pruning blockstore...")
	_, err = blockStore.PruneBlocks(lastDeleteHeight + 1)
	if err != nil {
		panic(err)
	}
	fmt.Println("Compacting blockstore, this could take a while...")
	err = blockStoreDB.(*db2.GoLevelDB).Compact(util.Range{})
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("Done pruning blockstore")
	loggerFile, _ := os.Open(os.DevNull)
	a := NewPocketCoreApp(nil, nil, nil, nil, log2.NewTMLogger(loggerFile), db)
	fmt.Println("Starting unsafe delete operation from blocks 0 to latestHeight")
	// get multistore
	ms := a.Store().(*rootmulti.Store)
	for i := int64(0); i < lastDeleteHeight; i++ {
		fmt.Println("Attempting to delete version: ", i)
		ms.DeleteVersions(i)
	}
	fmt.Println("Compacting AppDB, this could take a while...")
	err = db.(*db2.GoLevelDB).Compact(util.Range{})
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("done")
}
