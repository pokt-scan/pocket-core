package keeper

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pokt-network/pocket-core/crypto"
	sdk "github.com/pokt-network/pocket-core/types"
	pc "github.com/pokt-network/pocket-core/x/pocketcore/types"
)

// HandleRelay handles an api (read/write) request to a non-native (external) blockchain
func (k Keeper) HandleRelay(ctx sdk.Ctx, relay pc.Relay, isMesh bool) (*pc.RelayResponse, sdk.Error) {
	relayTimeStart := time.Now()

	sessionBlockHeight := relay.Proof.SessionBlockHeight

	if !k.IsProofSessionHeightWithinTolerance(ctx, sessionBlockHeight) {
		// For legacy support, we are intentionally returning the invalid block height error.
		return nil, pc.NewInvalidBlockHeightError(pc.ModuleName)
	}

	var servicerNode *pc.PocketNode
	// There is reference to node address so that way we don't have to recreate address twice for pre-leanpokt
	var servicerNodeAddr sdk.Address

	if pc.GlobalPocketConfig.LeanPocket {
		// if lean pocket enabled, grab the targeted servicer through the relay proof
		servicerRelayPublicKey, err := crypto.NewPublicKey(relay.Proof.ServicerPubKey)
		if err != nil {
			return nil, sdk.ErrInternal("Could not convert servicer hex to public key")
		}
		servicerNodeAddr = sdk.GetAddress(servicerRelayPublicKey)
		servicerNode, err = pc.GetPocketNodeByAddress(&servicerNodeAddr)
		if err != nil {
			return nil, sdk.ErrInternal("Failed to find correct servicer PK")
		}
	} else {
		// get self node (your validator) from the current state
		servicerNode = pc.GetPocketNode()
		servicerNodeAddr = servicerNode.GetAddress()
	}

	// retrieve the nonNative blockchains your node is hosting
	hostedBlockchains := k.GetHostedBlockchains()

	// ensure the validity of the relay
	maxPossibleRelays, e1 := relay.Validate(ctx, k.posKeeper, k.appKeeper, k, hostedBlockchains, sessionBlockHeight, servicerNode)
	if e1 != nil {
		if pc.GlobalPocketConfig.RelayErrors {
			ctx.Logger().Error(
				fmt.Sprintf("could not validate relay for app: %s for chainID: %v on node %s with error: %s",
					relay.Proof.Token.ApplicationPublicKey,
					relay.Proof.Blockchain,
					servicerNodeAddr.String(),
					e1.Error(),
				),
			)
			ctx.Logger().Debug(
				fmt.Sprintf(
					"could not validate relay for app: %s, for chainID %v on node %s, at session height: %v, with error: %s",
					relay.Proof.Token.ApplicationPublicKey,
					relay.Proof.Blockchain,
					servicerNodeAddr.String(),
					sessionBlockHeight,
					e1.Error(),
				),
			)
		}
		return nil, e1
	}
	// move this to a worker that will insert this proof in a serie style to avoid memory consumption and relay proof race conditions
	// https://github.com/pokt-network/pocket-core/issues/1457
	if evidenceWorker, ok := pc.GlobalEvidenceWorkerMap.Load(servicerNodeAddr.String()); ok {
		ctx.Logger().Debug(fmt.Sprintf("adding relay to evidence worker for address=%s", servicerNodeAddr.String()))
		evidenceWorker.Submit(func() {
			// store the proof before execution, because the proof corresponds to the previous relay
			relay.Proof.Store(maxPossibleRelays, servicerNode.EvidenceStore)
		})
	} else {
		ctx.Logger().Error(fmt.Sprintf("evidence worker not found for address=%s", servicerNodeAddr.String()))
	}

	// store the proof before execution, because the proof corresponds to the previous relay
	//relay.Proof.Store(maxPossibleRelays, node.EvidenceStore)

	var resp *pc.RelayResponse

	if !isMesh {
		// attempt to execute
		respPayload, err := relay.Execute(hostedBlockchains, &servicerNodeAddr)
		if err != nil {
			ctx.Logger().Error(fmt.Sprintf("could not send relay with error: %s", err.Error()))
			return nil, err
		}
		// generate response object
		resp = &pc.RelayResponse{
			Response: respPayload,
			Proof:    relay.Proof,
		}
		// sign the response
		sig, er := servicerNode.PrivateKey.Sign(resp.Hash())
		if er != nil {
			ctx.Logger().Error(
				fmt.Sprintf("could not sign response for address: %s with hash: %v, with error: %s",
					servicerNodeAddr.String(), resp.HashString(), er.Error()),
			)
			return nil, pc.NewKeybaseError(pc.ModuleName, er)
		}
		// attach the signature in hex to the response
		resp.Signature = hex.EncodeToString(sig)
	}
	// track the relay time
	relayTime := time.Since(relayTimeStart)
	// add to metrics
	addRelayMetricsFunc := func() {
		pc.GlobalServiceMetric().AddRelayTimingFor(relay.Proof.Blockchain, float64(relayTime.Milliseconds()), &servicerNodeAddr)
		pc.GlobalServiceMetric().AddRelayFor(relay.Proof.Blockchain, &servicerNodeAddr)
	}
	if pc.GlobalPocketConfig.LeanPocket {
		go addRelayMetricsFunc()
	} else {
		addRelayMetricsFunc()
	}
	return resp, nil
}

// "HandleChallenge" - Handles a client relay response challenge request
func (k Keeper) HandleChallenge(ctx sdk.Ctx, challenge pc.ChallengeProofInvalidData) (*pc.ChallengeResponse, sdk.Error) {

	var node *pc.PocketNode
	// There is reference to self address so that way we don't have to recreate address twice for pre-leanpokt
	var nodeAddress sdk.Address

	if pc.GlobalPocketConfig.LeanPocket {
		// try to retrieve a PocketNode that was part of session
		for _, r := range challenge.MajorityResponses {
			servicerRelayPublicKey, err := crypto.NewPublicKey(r.Proof.ServicerPubKey)
			if err != nil {
				continue
			}
			potentialNodeAddress := sdk.GetAddress(servicerRelayPublicKey)
			potentialNode, err := pc.GetPocketNodeByAddress(&nodeAddress)
			if err != nil || potentialNode == nil {
				continue
			}
			node = potentialNode
			nodeAddress = potentialNodeAddress
			break
		}
		if node == nil {
			return nil, pc.NewNodeNotInSessionError(pc.ModuleName)
		}
	} else {
		node = pc.GetPocketNode()
		nodeAddress = node.GetAddress()
	}

	sessionBlkHeight := k.GetLatestSessionBlockHeight(ctx)
	// get the session context
	sessionCtx, er := ctx.PrevCtx(sessionBlkHeight)
	if er != nil {
		return nil, sdk.ErrInternal(er.Error())
	}
	// get the application that staked on behalf of the client
	app, found := k.GetAppFromPublicKey(sessionCtx, challenge.MinorityResponse.Proof.Token.ApplicationPublicKey)
	if !found {
		return nil, pc.NewAppNotFoundError(pc.ModuleName)
	}
	// generate header
	header := pc.SessionHeader{
		ApplicationPubKey:  challenge.MinorityResponse.Proof.Token.ApplicationPublicKey,
		Chain:              challenge.MinorityResponse.Proof.Blockchain,
		SessionBlockHeight: sessionCtx.BlockHeight(),
	}
	// check cache
	session, found := pc.GetSession(header, node.SessionStore)
	// if not found generate the session
	if !found {
		var err sdk.Error
		blockHashBz, er := sessionCtx.BlockHash(k.Cdc, sessionCtx.BlockHeight())
		if er != nil {
			return nil, sdk.ErrInternal(er.Error())
		}
		session, err = pc.NewSession(sessionCtx, ctx, k.posKeeper, header, hex.EncodeToString(blockHashBz), int(k.SessionNodeCount(sessionCtx)))
		if err != nil {
			return nil, err
		}
		// add to cache
		pc.SetSession(session, node.SessionStore)
	}
	// validate the challenge
	err := challenge.ValidateLocal(header, app.GetMaxRelays(), app.GetChains(), int(k.SessionNodeCount(sessionCtx)), session.SessionNodes, nodeAddress, node.EvidenceStore)
	if err != nil {
		return nil, err
	}
	// store the challenge in memory
	challenge.Store(app.GetMaxRelays(), node.EvidenceStore)
	// update metric

	if pc.GlobalPocketConfig.LeanPocket {
		go pc.GlobalServiceMetric().AddChallengeFor(header.Chain, &nodeAddress)
	} else {
		pc.GlobalServiceMetric().AddChallengeFor(header.Chain, &nodeAddress)
	}

	return &pc.ChallengeResponse{Response: fmt.Sprintf("successfully stored challenge proof for %s", challenge.MinorityResponse.Proof.ServicerPubKey)}, nil
}

// HandleMeshSession - handles a request from a mesh node that runs minimum remote validations
func (k Keeper) HandleMeshSession(ctx sdk.Ctx, session pc.MeshSession) (*pc.MeshSessionResponse, sdk.Error) {
	// get the latest session block height because this relay will correspond with the latest session
	sessionBlockHeight := k.GetLatestSessionBlockHeight(ctx)
	var node *pc.PocketNode
	// There is reference to node address so that way we don't have to recreate address twice for pre-leanpokt
	var nodeAddress sdk.Address

	if pc.GlobalPocketConfig.LeanPocket {
		// if lean pocket enabled, grab the targeted servicer through the relay proof
		servicerRelayPublicKey, e1 := crypto.NewPublicKey(session.ServicerPubKey)
		if e1 != nil {
			return nil, sdk.ErrInternal("Could not convert servicer hex to public key")
		}
		nodeAddress = sdk.GetAddress(servicerRelayPublicKey)
		node, e1 = pc.GetPocketNodeByAddress(&nodeAddress)
		if e1 != nil {
			return nil, sdk.ErrInternal("Failed to find correct servicer PK")
		}
	} else {
		// get self node (your validator) from the current state
		node = pc.GetPocketNode()
		nodeAddress = node.GetAddress()
	}

	// retrieve the nonNative blockchains your node is hosting
	hostedBlockchains := k.GetHostedBlockchains()
	// ensure the validity of the relay
	remainingRelays, e2 := session.Validate(ctx, k.posKeeper, k.appKeeper, k, hostedBlockchains, sessionBlockHeight, node)
	if e2 != nil {
		if pc.GlobalPocketConfig.RelayErrors {
			ctx.Logger().Error(
				fmt.Sprintf("could not validate relay for app: %s for chainID: %v on node %s with error: %s",
					session.ApplicationPubKey,
					session.Blockchain,
					nodeAddress.String(),
					e2.Error(),
				),
			)
			ctx.Logger().Debug(
				fmt.Sprintf(
					"could not validate relay for app: %s, for chainID %v on node %s, at session height: %v, with error: %s",
					session.ApplicationPubKey,
					session.Blockchain,
					nodeAddress.String(),
					sessionBlockHeight,
					e2.Error(),
				),
			)
		}
		return nil, e2
	}

	dispatch, e3 := k.HandleDispatch(ctx, session.SessionHeader)

	if e3 != nil {
		return nil, e3
	}

	return &pc.MeshSessionResponse{
		Session:         *dispatch,
		RemainingRelays: remainingRelays.Int64(),
	}, nil
}
