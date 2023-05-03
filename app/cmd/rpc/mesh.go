package rpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/julienschmidt/httprouter"
	"github.com/pokt-network/pocket-core/app"
	"github.com/pokt-network/pocket-core/app/cmd/rpc/mesh"
	types4 "github.com/pokt-network/pocket-core/app/cmd/rpc/types"
	sdk "github.com/pokt-network/pocket-core/types"
	nodesTypes "github.com/pokt-network/pocket-core/x/nodes/types"
	pocketTypes "github.com/pokt-network/pocket-core/x/pocketcore/types"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// ++++++++++++++++++++ MESH CLIENT - PUBLIC ROUTES ++++++++++++++++++++

// meshNodeRelay - handle mesh node relay request, call handleRelay
// path: /v1/client/relay
func meshNodeRelay(c *fiber.Ctx) error {
	c.Accepts("application/json")

	var relay = pocketTypes.Relay{}

	if err := c.BodyParser(&relay); err != nil {
		response := mesh.RPCRelayResponse{
			Success: false,
			Error:   mesh.NewSdkErrorFromPocketSdkError(sdk.ErrInternal(err.Error())),
		}
		if e := c.JSON(response); e != nil {
			return fiber.NewError(500, "unable to serialize response", "err", e.Error())
		}
		c.Status(400)
		return nil
	}

	mesh.GetLogger().Debug(fmt.Sprintf("handling relay %s", relay.RequestHashString()))
	res, dispatch, err := mesh.HandleRelay(&relay)

	if err != nil {
		response := mesh.RPCRelayResponse{
			Success:  false,
			Error:    mesh.NewSdkErrorFromPocketSdkError(sdk.ErrInternal(err.Error())),
			Dispatch: dispatch,
		}

		if e := c.JSON(response); e != nil {
			return fiber.NewError(500, "unable to serialize response", "err", e.Error())
		}

		c.Status(400)
		return nil
	}

	response := RPCRelayResponse{
		Signature: res.Signature,
		Response:  res.Response,
	}

	mesh.GetLogger().Debug(fmt.Sprintf("relay %s done", relay.RequestHashString()))
	return mesh.WriteResponse(&response, c)
}

// meshSimulateRelay - handle a simulated relay to test connectivity to the chains that this should be serving.
// this will only be enabled if start node with --simulateRelays
// path: /v1/client/sim
func meshSimulateRelay(c *fiber.Ctx) error {
	var params = SimRelayParams{}
	if er := c.BodyParser(&params); er != nil {
		//WriteErrorResponse(w, 400, err.Error())
		return mesh.WriteError(400, er.Error(), c)
	}

	chain, er := mesh.GetChains().GetChain(params.RelayNetworkID)
	if er != nil {
		//WriteErrorResponse(w, 400, err.Error())
		return mesh.WriteError(400, er.Error(), c)
	}

	_url := strings.Trim(chain.URL, `/`)
	if len(params.Payload.Path) > 0 {
		_url = _url + "/" + strings.Trim(params.Payload.Path, `/`)
	}

	mesh.GetLogger().Debug(
		fmt.Sprintf(
			"executing simulated relay of chain %s",
			chain.ID,
		),
	)
	// do basic http request on the relay
	res, e, _ := mesh.ExecuteBlockchainHTTPRequest(
		params.Payload.Data, _url, app.GlobalMeshConfig.UserAgent,
		chain.BasicAuth, params.Payload.Method, params.Payload.Headers,
	)
	if e != nil {
		//WriteErrorResponse(w, 400, er.Error())
		return mesh.WriteError(400, e.Error(), c)
	}

	return mesh.WriteResponse(res, c)
}

// ++++++++++++++++++++ MESH CLIENT - PRIVATE ROUTES ++++++++++++++++++++

// meshHealth - handle mesh health request
// path: /v1/private/mesh/health
func meshHealth(c *fiber.Ctx) error {
	res := mesh.HealthResponse{
		Version:   mesh.AppVersion,
		Servicers: mesh.ServicersSize(),
		FullNodes: mesh.NodesSize(),
	}

	return mesh.WriteResponse(res, c)
}

// meshChains - return load chains from app.GlobalMeshConfig.ChainsName file
// path: /v1/private/mesh/chains
func meshChains(c *fiber.Ctx) error {
	blockchains := make([]pocketTypes.HostedBlockchain, 0)

	for _, chain := range mesh.GetChains().M {
		blockchains = append(blockchains, chain)
	}

	return mesh.WriteResponse(blockchains, c)
}

// meshServicerNode - return servicer node configured by servicer_priv_key.json - return address
// path: /v1/private/mesh/servicer
func meshServicerNode(c *fiber.Ctx) error {
	servicers := make([]types4.PublicPocketNode, 0)

	for _, a := range mesh.GetServicerLists() {
		servicers = append(servicers, types4.PublicPocketNode{
			Address: a,
		})
	}

	return mesh.WriteResponse(servicers, c)
}

// meshUpdateChains - update chains in memory and also chains.json file.
// path: /v1/private/mesh/updatechains
func meshUpdateChains(c *fiber.Ctx) error {
	var hostedChainsSlice []pocketTypes.HostedBlockchain

	if er := c.BodyParser(&hostedChainsSlice); er != nil {
		return mesh.WriteError(400, er.Error(), c)
	}

	chains, e := mesh.UpdateChains(hostedChainsSlice)

	if e != nil {
		return mesh.WriteError(400, e.Error(), c)
	}

	return mesh.WriteResponse(chains.M, c)
}

// meshStop - gracefully stop mesh rpc server. Also, this should stop new relays and wait/flush all pending relays, otherwise they will get loose.
// path: /v1/private/mesh/stop
func meshStop(c *fiber.Ctx) error {
	e := mesh.WriteResponse(map[string]string{}, c)
	if e != nil {
		mesh.GetLogger().Error("unable to write response before stop server; continue shutting down it...", "err", e.Error())
	}
	mesh.StopRPC()
	fmt.Println("Stop Successful, PID:" + fmt.Sprint(os.Getpid()))
	os.Exit(0)
	return nil
}

// ++++++++++++++++++++ POKT CLIENT - PRIVATE ROUTES ++++++++++++++++++++

// meshServicerNodeRelay - receive relays that was processed by mesh node
// path: /v1/private/mesh/relay
func meshServicerNodeRelay(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var relay = pocketTypes.Relay{}

	if cors(&w, r) {
		return
	}

	token := r.Header.Get("Authorization")
	if token != app.AuthToken.Value {
		WriteErrorResponse(w, 401, "wrong authtoken: "+token)
		return
	}

	verify := r.URL.Query().Get("verify")
	if verify == "true" {
		code := 200
		// useful just to test that mesh node is able to reach servicer
		response := mesh.RPCRelayResult{
			Success:  true,
			Error:    nil,
			Dispatch: nil,
		}

		j, _ := json.Marshal(response)
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, code)
		return
	}

	if err := PopModel(w, r, ps, &relay); err != nil {
		response := RPCRelayErrorResponse{
			Error: err,
		}
		j, _ := json.Marshal(response)
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, 400)
		return
	}

	_, dispatch, err := app.PCA.HandleRelay(relay, true)
	if err != nil {
		response := mesh.RPCRelayResult{
			Success:  false,
			Error:    err,
			Dispatch: dispatch,
		}
		j, _ := json.Marshal(response)
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, 400)
		return
	}

	response := mesh.RPCRelayResult{
		Success:  true,
		Dispatch: dispatch,
	}
	j, er := json.Marshal(response)
	if er != nil {
		WriteErrorResponse(w, 400, er.Error())
		return
	}

	WriteJSONResponse(w, string(j), r.URL.Path, r.Host)
}

// meshServicerNodeSession - receive requests from mesh node to validate a session for an app/servicer/blockchain on the servicer node data
// path: /v1/private/mesh/session
func meshServicerNodeSession(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var session pocketTypes.MeshSession

	token := r.Header.Get("Authorization")
	if token != app.AuthToken.Value {
		WriteErrorResponse(w, 401, "wrong authtoken: "+token)
		return
	}

	verify := r.URL.Query().Get("verify")
	if verify == "true" {
		code := 200
		// useful just to test that mesh node is able to reach servicer
		response := mesh.RPCSessionResult{
			Success:  true,
			Error:    nil,
			Dispatch: nil,
		}

		j, _ := json.Marshal(response)
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, code)
		return
	}

	if err := PopModel(w, r, ps, &session); err != nil {
		WriteErrorResponse(w, 400, err.Error())
		return
	}

	res, err := app.PCA.HandleMeshSession(session)

	if err != nil {
		response := mesh.RPCSessionResult{
			Success: false,
			Error:   mesh.NewSdkErrorFromPocketSdkError(err),
		}
		j, _ := json.Marshal(response)
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, 400)
		return
	}

	dispatch := mesh.DispatchResponse{
		BlockHeight: res.Session.BlockHeight,
		Session: mesh.DispatchSession{
			Header: res.Session.Session.SessionHeader,
			Key:    hex.EncodeToString(res.Session.Session.SessionKey),
			Nodes:  make([]mesh.DispatchSessionNode, 0),
		},
	}

	for i := range res.Session.Session.SessionNodes {
		sNode, ok := res.Session.Session.SessionNodes[i].(nodesTypes.Validator)
		if !ok {
			continue
		}
		dispatch.Session.Nodes = append(dispatch.Session.Nodes, mesh.DispatchSessionNode{
			Address:       sNode.Address.String(),
			Chains:        sNode.Chains,
			Jailed:        sNode.Jailed,
			OutputAddress: sNode.OutputAddress.String(),
			PublicKey:     sNode.PublicKey.String(),
			ServiceUrl:    sNode.ServiceURL,
			Status:        sNode.Status,
			Tokens:        sNode.GetTokens().String(),
			UnstakingTime: sNode.UnstakingCompletionTime,
		})
	}

	response := mesh.RPCSessionResult{
		Success:         true,
		Dispatch:        &dispatch,
		RemainingRelays: json.Number(strconv.FormatInt(res.RemainingRelays, 10)),
	}
	j, er := json.Marshal(response)
	if er != nil {
		WriteErrorResponse(w, 400, er.Error())
		return
	}

	WriteJSONResponse(w, string(j), r.URL.Path, r.Host)
}

// meshServicerNodeCheck - receive requests from mesh node to validate servicers, chains and health status
// path: /v1/private/mesh/check
func meshServicerNodeCheck(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var checkPayload mesh.CheckPayload

	token := r.Header.Get("Authorization")
	if token != app.AuthToken.Value {
		WriteErrorResponse(w, 401, "wrong authtoken: "+token)
		return
	}

	verify := r.URL.Query().Get("verify")
	// useful just to test that mesh node is able to reach servicer - this payload should be ignored
	if verify == "true" {
		code := 200
		j, _ := json.Marshal(map[string]interface{}{})
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, code)
		return
	}

	if err := PopModel(w, r, ps, &checkPayload); err != nil {
		WriteErrorResponse(w, 400, err.Error())
		return
	}

	chainsMap, err := app.PCA.QueryHostedChains()
	health := app.PCA.QueryHealth(APIVersion)

	if err != nil {
		response := mesh.RPCSessionResult{
			Success: false,
			Error:   mesh.NewSdkErrorFromPocketSdkError(sdk.ErrInternal(err.Error())),
		}
		j, _ := json.Marshal(response)
		WriteJSONResponseWithCode(w, string(j), r.URL.Path, r.Host, 400)
		return
	}

	response := mesh.CheckResponse{
		Success:        true,
		Status:         health,
		Servicers:      true,
		Chains:         true,
		WrongServicers: make([]string, 0),
		WrongChains:    make([]string, 0),
	}

	for _, address := range checkPayload.Servicers {
		if pocketTypes.GlobalPocketConfig.LeanPocket {
			// if lean pocket enabled, grab the targeted servicer through the relay proof
			nodeAddress, e1 := sdk.AddressFromHex(address)
			if e1 != nil {
				WriteErrorResponse(w, 400, "could not convert servicer hex")
				return
			}
			_, e2 := pocketTypes.GetPocketNodeByAddress(&nodeAddress)
			if e2 != nil {
				response.Servicers = false
				response.WrongServicers = append(response.WrongServicers, address)
			}
		} else {
			// get self node (your validator) from the current state
			node := pocketTypes.GetPocketNode()
			nodeAddress := node.GetAddress()
			if nodeAddress.String() != address {
				response.Servicers = false
				response.WrongServicers = append(response.WrongServicers, address)
			}
		}
	}

	for _, chain := range checkPayload.Chains {
		if _, ok := chainsMap[chain]; !ok {
			response.Chains = false
			response.WrongChains = append(response.WrongChains, chain)
		}
	}

	j, er := json.Marshal(response)
	if er != nil {
		WriteErrorResponse(w, 400, er.Error())
		return
	}

	WriteJSONResponse(w, string(j), r.URL.Path, r.Host)
}

// getMeshRoutes - return routes that will be handled/proxied by mesh rpc server
func getMeshRoutes(simulation bool) mesh.Routes {
	routes := mesh.Routes{
		// Proxy
		mesh.Route{Name: "AppVersion", Method: "GET", Path: "/v1", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "Health", Method: "GET", Path: "/v1/health", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "Challenge", Method: "POST", Path: "/v1/client/challenge", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "ChallengeCORS", Method: "OPTIONS", Path: "/v1/client/challenge", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "HandleDispatch", Method: "POST", Path: "/v1/client/dispatch", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "HandleDispatchCORS", Method: "OPTIONS", Path: "/v1/client/dispatch", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "SendRawTx", Method: "POST", Path: "/v1/client/rawtx", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "Stop", Method: "POST", Path: "/v1/private/stop", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryChains", Method: "POST", Path: "/v1/private/chains", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryAccount", Method: "POST", Path: "/v1/query/account", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryAccounts", Method: "POST", Path: "/v1/query/accounts", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryAccountTxs", Method: "POST", Path: "/v1/query/accounttxs", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryACL", Method: "POST", Path: "/v1/query/acl", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryAllParams", Method: "POST", Path: "/v1/query/allparams", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryApp", Method: "POST", Path: "/v1/query/app", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryAppParams", Method: "POST", Path: "/v1/query/appparams", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryApps", Method: "POST", Path: "/v1/query/apps", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryBalance", Method: "POST", Path: "/v1/query/balance", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryBlock", Method: "POST", Path: "/v1/query/block", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryBlockTxs", Method: "POST", Path: "/v1/query/blocktxs", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryDAOOwner", Method: "POST", Path: "/v1/query/daoowner", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryHeight", Method: "POST", Path: "/v1/query/height", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryNode", Method: "POST", Path: "/v1/query/node", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryNodeClaim", Method: "POST", Path: "/v1/query/nodeclaim", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryNodeClaims", Method: "POST", Path: "/v1/query/nodeclaims", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryNodeParams", Method: "POST", Path: "/v1/query/nodeparams", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryNodes", Method: "POST", Path: "/v1/query/nodes", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryParam", Method: "POST", Path: "/v1/query/param", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryPocketParams", Method: "POST", Path: "/v1/query/pocketparams", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryState", Method: "POST", Path: "/v1/query/state", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QuerySupply", Method: "POST", Path: "/v1/query/supply", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QuerySupportedChains", Method: "POST", Path: "/v1/query/supportedchains", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryTX", Method: "POST", Path: "/v1/query/tx", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryUpgrade", Method: "POST", Path: "/v1/query/upgrade", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QuerySigningInfo", Method: "POST", Path: "/v1/query/signinginfo", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "LocalNodes", Method: "POST", Path: "/v1/private/nodes", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryUnconfirmedTxs", Method: "POST", Path: "/v1/query/unconfirmedtxs", HandlerFunc: mesh.ProxyRequest},
		mesh.Route{Name: "QueryUnconfirmedTx", Method: "POST", Path: "/v1/query/unconfirmedtx", HandlerFunc: mesh.ProxyRequest},
		// mesh public route to handle relays
		mesh.Route{Name: "MeshService", Method: "POST", Path: "/v1/client/relay", HandlerFunc: meshNodeRelay},
		// mesh private routes
		mesh.Route{Name: "MeshHealth", Method: "GET", Path: "/v1/private/mesh/health", HandlerFunc: meshHealth, Auth: true},
		mesh.Route{Name: "QueryMeshNodeChains", Method: "POST", Path: "/v1/private/mesh/chains", HandlerFunc: meshChains, Auth: true},
		mesh.Route{Name: "MeshNodeServicer", Method: "POST", Path: "/v1/private/mesh/servicers", HandlerFunc: meshServicerNode, Auth: true},
		mesh.Route{Name: "UpdateMeshNodeChains", Method: "POST", Path: "/v1/private/mesh/updatechains", HandlerFunc: meshUpdateChains, Auth: true},
		mesh.Route{Name: "StopMeshNode", Method: "POST", Path: "/v1/private/mesh/stop", HandlerFunc: meshStop, Auth: true},
	}

	// check if simulation is turn on
	if simulation {
		simRoute := mesh.Route{Name: "SimulateRequest", Method: "POST", Path: "/v1/client/sim", HandlerFunc: meshSimulateRelay}
		routes = append(routes, simRoute)
	}

	return routes
}

// GetServicerMeshRoutes - return routes that need to be added to servicer to allow mesh node to communicate with.
func GetServicerMeshRoutes() Routes {
	routes := Routes{
		{Name: "MeshRelay", Method: "POST", Path: mesh.ServicerRelayEndpoint, HandlerFunc: meshServicerNodeRelay},
		{Name: "MeshSession", Method: "POST", Path: mesh.ServicerSessionEndpoint, HandlerFunc: meshServicerNodeSession},
		{Name: "MeshCheck", Method: "POST", Path: mesh.ServicerCheckEndpoint, HandlerFunc: meshServicerNodeCheck},
	}

	return routes
}

// StartMeshRPC - encapsulate mesh.StartRPC
func StartMeshRPC(simulation bool) {
	mesh.StartRPC(getMeshRoutes(simulation))
}
