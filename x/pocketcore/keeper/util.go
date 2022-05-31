package keeper

import (
	"bytes"
	"fmt"
	"github.com/pokt-network/pocket-core/crypto"
	sdk "github.com/pokt-network/pocket-core/types"
	"github.com/pokt-network/pocket-core/x/pocketcore/types"
	tmCrypto "github.com/tendermint/tendermint/crypto"
)

// "GetPKFromFile" - Returns the private key object from a file
func (k Keeper) GetPKFromFile(ctx sdk.Ctx, address sdk.Address) (crypto.PrivateKey, error) {
	// get the Private validator key from the file
	pvKey, err := types.GetPVKeyFile()
	if err != nil {
		return nil, err
	}
	key := tmCrypto.PrivKey(nil)
	for _, k := range pvKey {
		if bytes.Equal(k.Address.Bytes(), address.Bytes()) {
			key = k.PrivKey
			break
		}
	}
	if key == nil {
		return nil, fmt.Errorf("private key is not found in the file")
	}
	// convert the privKey to a private key object (compatible interface)
	pk, er := crypto.PrivKeyToPrivateKey(key)
	if er != nil {
		return nil, er
	}
	return pk, nil
}

// "GetPKsFromFile" - Returns the private key objects from a file
func (k Keeper) GetPKsFromFile(ctx sdk.Ctx) ([]crypto.PrivateKey, error) {
	// get the Private validator key from the file
	pvKey, err := types.GetPVKeyFile()
	if err != nil {
		return nil, err
	}
	var key []crypto.PrivateKey
	for _, k := range pvKey {
		pk, err := crypto.PrivKeyToPrivateKey(k.PrivKey)
		if err != nil {
			return nil, err
		}
		key = append(key, pk)
	}
	return key, nil
}
