package keeper

import (
	"bytes"
	"fmt"
	log2 "log"
	"sort"

	"github.com/pokt-network/pocket-core/codec"
	sdk "github.com/pokt-network/pocket-core/types"
	"github.com/pokt-network/pocket-core/x/nodes/types"
	"github.com/tendermint/tendermint/libs/log"
)

// Implements ValidatorSet interface
var _ types.ValidatorSet = Keeper{}

// Keeper of the staking store
type Keeper struct {
	storeKey      sdk.StoreKey
	Cdc           *codec.Codec
	AccountKeeper types.AuthKeeper
	PocketKeeper  types.PocketKeeper // todo combine all modules
	Paramstore    sdk.Subspace
	// codespace
	codespace sdk.CodespaceType
	// Cache
	validatorCache *sdk.Cache
}

var (
	ValidatorsByChainLatestHeight int64
	ValidatorsByChain             map[int64]map[string][]sdk.Address
)

// NewKeeper creates a new staking Keeper instance
func NewKeeper(cdc *codec.Codec, key sdk.StoreKey, accountKeeper types.AuthKeeper,
	paramstore sdk.Subspace, codespace sdk.CodespaceType) Keeper {
	// ensure staked module accounts are set
	if addr := accountKeeper.GetModuleAddress(types.StakedPoolName); addr == nil {
		log2.Fatal(fmt.Errorf("%s module account has not been set", types.StakedPoolName))
	}
	cache := sdk.NewCache(int(types.ValidatorCacheSize))
	return Keeper{
		storeKey:       key,
		AccountKeeper:  accountKeeper,
		Paramstore:     paramstore.WithKeyTable(ParamKeyTable()),
		codespace:      codespace,
		validatorCache: cache,
		Cdc:            cdc,
	}
}

func (k *Keeper) PopulateValidatorsByChainCache(ctx sdk.Ctx) {
	ValidatorsByChain = make(map[int64]map[string][]sdk.Address)
	blkHeight := ctx.BlockHeight()
	min := blkHeight - 25
	if min < 1 {
		min = 1
	}
	k.Logger(ctx).Info("Populating validator cache")
	for i := min; i <= blkHeight; i++ {
		k.Logger(ctx).Info("Height", i, "up to", blkHeight)
		ValidatorsByChain[i] = make(map[string][]sdk.Address)
		prevCtx, err := ctx.PrevCtx(i)
		if err != nil {
			panic(fmt.Sprintf("unable to get prevCtx for height %d in populateValidatorsByChain", i))
		}
		sv := k.GetAllValidators(prevCtx)
		for _, v := range sv {
			if !v.IsStaked() {
				continue
			}
			for _, c := range v.GetChains() {
				ValidatorsByChain[i][c] = append(ValidatorsByChain[i][c], v.GetAddress())
			}
		}
		// sort in leveldb like fashion
		for c, addrSlice := range ValidatorsByChain[i] {
			sort.Slice(addrSlice, func(a, b int) bool {
				return bytes.Compare(addrSlice[a], addrSlice[b]) == -1
			})
			ValidatorsByChain[i][c] = addrSlice
		}
	}
	ValidatorsByChainLatestHeight = blkHeight
}

func (k *Keeper) UpdateValidatorsByChainCache(ctx sdk.Ctx, address sdk.Address, networkID string, op string) {
	if ValidatorsByChainLatestHeight != ctx.BlockHeight() {
		panic("Update to non-latest block height called")
	}
	if ValidatorsByChain[ctx.BlockHeight()] != nil {
		isPresent := true
		addrs := ValidatorsByChain[ctx.BlockHeight()][networkID]
		i := sort.Search(len(addrs), func(a int) bool { return bytes.Compare(addrs[a], address) >= 0 })
		if i >= len(addrs) || !bytes.Equal(addrs[i], address) {
			isPresent = false
		}
		if isPresent {
			switch op {
			case "set":
				// nothing
			case "delete":
				addrs = append(addrs[:i], addrs[i+1:]...)
			default:
				panic("invalid op in updateValidtorsByChainCache")
			}
		} else {
			switch op {
			case "set":
				addrs = append(addrs, nil)
				copy(addrs[i+1:], addrs[i:])
				addrs[i] = address
			case "delete":
				// nothing
			default:
				panic("invalid op in updateValidtorsByChainCache")
			}
		}
		ValidatorsByChain[ctx.BlockHeight()][networkID] = addrs
	}
}

func (k *Keeper) IncrementValidatorsByChainCache(ctx sdk.Ctx) {
	blkHeight := ctx.BlockHeight()
	// last height becomes next
	ValidatorsByChain[blkHeight+1] = make(map[string][]sdk.Address)
	for c, s := range ValidatorsByChain[blkHeight] {
		newS := make([]sdk.Address, len(s))
		copy(newS, s)
		ValidatorsByChain[blkHeight+1][c] = newS
	}
	ValidatorsByChainLatestHeight = blkHeight + 1
	// prune last height
	pruneHeight := blkHeight - 25
	if pruneHeight < 0 {
		return
	}
	delete(ValidatorsByChain, pruneHeight)
}

// Logger - returns a module-specific logger.
func (k Keeper) Logger(ctx sdk.Ctx) log.Logger {
	return ctx.Logger().With("module", fmt.Sprintf("x/%s", types.ModuleName))
}

// Codespace - Retrieve the codespace
func (k Keeper) Codespace() sdk.CodespaceType {
	return k.codespace
}

func (k Keeper) UpgradeCodec(ctx sdk.Ctx) {
	if ctx.IsOnUpgradeHeight() {
		k.ConvertState(ctx)
	}
}

func (k Keeper) ConvertState(ctx sdk.Ctx) {
	k.Cdc.SetUpgradeOverride(false)
	params := k.GetParams(ctx)
	prevStateTotalPower := k.PrevStateValidatorsPower(ctx)
	validators := k.GetAllValidators(ctx)
	waitingValidators := k.GetWaitingValidators(ctx)
	prevProposer := k.GetPreviousProposer(ctx)
	var prevStateValidatorPowers []types.PrevStatePowerMapping
	k.IterateAndExecuteOverPrevStateValsByPower(ctx, func(addr sdk.Address, power int64) (stop bool) {
		prevStateValidatorPowers = append(prevStateValidatorPowers, types.PrevStatePowerMapping{Address: addr, Power: power})
		return false
	})
	signingInfos := make([]types.ValidatorSigningInfo, 0)
	k.IterateAndExecuteOverValSigningInfo(ctx, func(address sdk.Address, info types.ValidatorSigningInfo) (stop bool) {
		signingInfos = append(signingInfos, info)
		return false
	})
	err := k.UpgradeMissedBlocksArray(ctx, validators) // TODO might be able to remove missed array code
	if err != nil {
		panic(err)
	}
	k.Cdc.SetUpgradeOverride(true)
	// custom logic for minSignedPerWindow
	params.MinSignedPerWindow = params.MinSignedPerWindow.QuoInt64(params.SignedBlocksWindow)
	k.SetParams(ctx, params)
	k.SetPrevStateValidatorsPower(ctx, prevStateTotalPower)
	k.SetWaitingValidators(ctx, waitingValidators)
	k.SetValidators(ctx, validators)
	k.SetPreviousProposer(ctx, prevProposer)
	k.SetValidatorSigningInfos(ctx, signingInfos)
	k.Cdc.DisableUpgradeOverride()
}
