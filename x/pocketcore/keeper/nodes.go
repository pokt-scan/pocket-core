package keeper

import (
	"bytes"
	"fmt"
	"github.com/pokt-network/pocket-core/crypto"
	sdk "github.com/pokt-network/pocket-core/types"
	"github.com/pokt-network/pocket-core/x/nodes/exported"
	pc "github.com/pokt-network/pocket-core/x/pocketcore/types"
)

// "GetNode" - Gets a node from the state storage
func (k Keeper) GetNode(ctx sdk.Ctx, address sdk.Address) (n exported.ValidatorI, found bool) {
	n = k.posKeeper.Validator(ctx, address)
	if n == nil {
		return n, false
	}
	return n, true
}

func (k Keeper) GetSelfAddress(ctx sdk.Ctx) (addrs sdk.Addresses) {
	pvKey, err := pc.GetPVKeyFile()
	if err != nil {
		ctx.Logger().Error("Unable to retrieve pk from file: " + err.Error())
		return nil
	}
	for _, k := range pvKey {
		addrs = append(addrs, sdk.Address(k.Address))
	}
	return
}

func (k Keeper) GetSelfPrivKey(ctx sdk.Ctx) ([]crypto.PrivateKey, sdk.Error) {
	// get the private key from the private validator file
	pk, er := k.GetPKsFromFile(ctx)
	if er != nil {
		return nil, pc.NewKeybaseError(pc.ModuleName, er)
	}
	return pk, nil
}

func (k Keeper) GetSelfPrivKeyFromAddr(ctx sdk.Ctx, signer sdk.Address) (crypto.PrivateKey, sdk.Error) {
	// get the private key from the private validator file
	pvKey, err := pc.GetPVKeyFile()
	if err != nil {
		ctx.Logger().Error("Unable to retrieve pk from file: " + err.Error())
		return nil, err
	}
	for _, k := range pvKey {
		if bytes.Equal(k.Address.Bytes(), signer) {
			key, err := crypto.PrivKeyToPrivateKey(k.PrivKey)
			if err != nil {
				return nil, sdk.ErrInternal(err.Error())
			}
			return key, nil
		}
	}
	return nil, sdk.ErrInternal(fmt.Sprintf("private key not found in file for address: %s", signer.String()))
}

// "AwardCoinsForRelays" - Award coins to nodes for relays completed using the nodes keeper
func (k Keeper) AwardCoinsForRelays(ctx sdk.Ctx, relays int64, toAddr sdk.Address) sdk.BigInt {
	return k.posKeeper.RewardForRelays(ctx, sdk.NewInt(relays), toAddr)
}

// "BurnCoinsForChallenges" - Executes the burn for challenge function in the nodes module
func (k Keeper) BurnCoinsForChallenges(ctx sdk.Ctx, relays int64, toAddr sdk.Address) {
	k.posKeeper.BurnForChallenge(ctx, sdk.NewInt(relays), toAddr)
}
