package mesh

import (
	"context"
	"errors"
	"fmt"
	"github.com/akrylysov/pogreb"
	"github.com/ansrivas/fiberprometheus/v2"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/timeout"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pokt-network/pocket-core/app"
	sdk "github.com/pokt-network/pocket-core/types"
	pocketTypes "github.com/pokt-network/pocket-core/x/pocketcore/types"
	"github.com/puzpuzpuz/xsync"
	"github.com/robfig/cron/v3"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/valyala/fasthttp"
	log2 "log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const (
	ModuleName              = "pocketcore"
	ServicerHeader          = "X-Servicer"
	ServicerRelayEndpoint   = "/v1/private/mesh/relay"
	ServicerSessionEndpoint = "/v1/private/mesh/session"
	ServicerCheckEndpoint   = "/v1/private/mesh/check"
	AppVersion              = "BETA-0.4.0-fiber"
)

var (
	fiberApp *fiber.App
	//srv               *http.Server
	finish            context.CancelFunc
	logger            log.Logger
	chainsClient      *fasthttp.Client
	servicerClient    *fasthttp.Client
	relaysClient      *retryablehttp.Client
	relaysCacheDb     *pogreb.DB
	servicerMap       = xsync.NewMapOf[*servicer]()
	nodesMap          = xsync.NewMapOf[*fullNode]()
	servicerList      []string
	chains            *pocketTypes.HostedBlockchains
	meshAuthToken     sdk.AuthToken
	servicerAuthToken sdk.AuthToken
	cronJobs          *cron.Cron
	mutex             = sync.Mutex{}
)

// validate payload
//	modulename: pocketcore CodeEmptyPayloadDataError = 25
// ensures the block height is within the acceptable range
//	modulename: pocketcore CodeOutOfSyncRequestError            = 75
// validate the relay merkleHash = request merkleHash
// 	modulename: pocketcore CodeRequestHash                      = 74
// ensure the blockchain is supported locally
// 	CodeUnsupportedBlockchainNodeError   = 26
// ensure session block height == one in the relay proof
// 	CodeInvalidBlockHeightError          = 60
// get the session context
// 	CodeInternal              CodeType = 1
// get the application that staked on behalf of the client
// 	CodeAppNotFoundError                 = 45
// validate unique relay
// 	CodeEvidenceSealed                   = 90
// get evidence key by proof
// 	CodeDuplicateProofError              = 37
// validate not over service
// 	CodeOverServiceError                 = 71
// "ValidateLocal" - Validates the proof object, where the owner of the proof is the local node
// 	CodeInvalidBlockHeightError          = 60
// 	CodePublKeyDecodeError               = 6
// 	CodePubKeySizeError                  = 42
// 	CodeNewHexDecodeError                = 52
// 	CodeEmptyBlockHashError              = 23
// 	CodeInvalidHashLengthError           = 62
// 	CodeInvalidEntropyError              = 29
// 	CodeInvalidTokenError                = 4
// 	CodeSigDecodeError                   = 39
// 	CodeInvalidSignatureSizeError        = 38
// 	CodePublKeyDecodeError               = 6
// 	CodeMsgDecodeError                   = 40
// 	CodeInvalidSigError                  = 41
// 	CodeInvalidEntropyError              = 29
// 	CodeInvalidNodePubKeyError           = 34
// 	CodeUnsupportedBlockchainAppError    = 13
var invalidCodes = []sdk.CodeType{
	pocketTypes.CodeRequestHash,
	pocketTypes.CodeAppNotFoundError,
	pocketTypes.CodeEvidenceSealed,
	pocketTypes.CodeOverServiceError,
	pocketTypes.CodeOutOfSyncRequestError,
	pocketTypes.CodeInvalidBlockHeightError,
}

// StopRPC - stop http server
func StopRPC() {
	// stop receiving new requests
	logger.Info("stopping http server...")
	if fiberApp.Server() != nil {
		if err := fiberApp.ShutdownWithContext(context.Background()); err != nil {
			logger.Error(fmt.Sprintf("http server shutdown error: %s", err.Error()))
		}
	}
	logger.Info("http server stopped!")

	// close relays cache db
	logger.Info("stopping relays cache database...")
	if err := relaysCacheDb.Close(); err != nil {
		logger.Error(fmt.Sprintf("relays cache db shutdown error: %s", err.Error()))
	}
	logger.Info("relays cache database stopped!")

	// stop accepting new tasks and signal all workers to stop processing new tasks. Tasks being processed by workers
	// will continue until completion unless the process is terminated.
	logger.Info("stopping worker pools...")
	nodesMap.Range(func(key string, node *fullNode) bool {
		node.stop()
		return true
	})
	logger.Info("worker pools stopped!")

	logger.Info("stopping clean session cron job")
	cronJobs.Stop()
	logger.Info("clean session job stopped!")

	// Stop prometheus server
	StopPrometheusServer()
}

// StartRPC - Start mesh rpc server
func StartRPC(routes Routes) {
	fiberApp = fiber.New(fiber.Config{
		ServerHeader:  "Geo-Mesh",
		AppName:       app.AppVersion,
		Prefork:       false,
		CaseSensitive: true,
		StrictRouting: true,
		JSONEncoder:   json.Marshal,
		JSONDecoder:   json.Unmarshal,
		ReadTimeout:   time.Duration(app.GlobalMeshConfig.ClientRPCReadTimeout) * time.Millisecond,
		WriteTimeout:  time.Duration(app.GlobalMeshConfig.ClientRPCWriteTimeout) * time.Millisecond,
	})

	// load middlewares
	fiberApp.Use(recover.New())
	// only allow application/json content
	fiberApp.Use(func(c *fiber.Ctx) error {
		c.Accepts("application/json")
		return c.Next()
	})

	labels := map[string]string{
		InstanceMoniker: app.GlobalMeshConfig.MetricsMoniker,
		StatTypeLabel:   "server",
	}
	fiberPrometheus := fiberprometheus.NewWithLabels(labels, ModuleName, ServiceMetricsNamespace)
	fiberPrometheus.RegisterAt(fiberApp, "/metrics")
	fiberApp.Use(fiberPrometheus.Middleware)

	fiberApp.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed, // 1
	}))
	fiberApp.Use(cors.New())

	// inject routes to fiber app
	for i := range routes {
		route := &routes[i]
		if route.Auth {
			// load is authorized middleware
			fiberApp.Use(route.Path, IsAuthorized)
		}

		fiberApp.Add(
			route.Method,
			route.Path,
			timeout.NewWithContext(
				route.HandlerFunc,
				time.Duration(app.GlobalMeshConfig.ClientRPCTimeout)*time.Millisecond,
				errors.New(
					fmt.Sprintf(
						"request to %s timeout after %d ms",
						route.Path,
						app.GlobalMeshConfig.ClientRPCTimeout,
					),
				),
			),
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	finish = cancel
	defer cancel()
	// initialize logger
	logger = initLogger()
	// initialize pseudo random to choose servicer url
	rand.Seed(time.Now().Unix())
	// load auth token files (servicer and mesh node)
	loadAuthTokens()
	// instantiate all the http clients used to call Chains and Servicer
	prepareHttpClients()
	// retrieve the nonNative blockchains your node is hosting
	chains = loadHostedChains()
	// load chain name map use on metrics. this will not raise or throw an error.
	loadChainsNameMap()
	// turn on chains hot reload
	go initKeysHotReload()
	go initChainsHotReload()
	// initialize prometheus metrics
	StartPrometheusServer()
	// read servicer
	totalNodes, totalServicers := loadServicerNodes()
	// check servicers are reachable at required endpoints
	connectivityChecks(mapset.NewSet[string]())
	// initialize crons
	initCrons()
	// bootstrap cache
	initCache()

	//srv = &http.Server{
	//	ReadTimeout:       time.Duration(app.GlobalMeshConfig.ClientRPCReadTimeout) * time.Millisecond,
	//	ReadHeaderTimeout: time.Duration(app.GlobalMeshConfig.ClientRPCReadHeaderTimeout) * time.Millisecond,
	//	WriteTimeout:      time.Duration(app.GlobalMeshConfig.ClientRPCWriteTimeout) * time.Millisecond,
	//	Addr:              ":" + app.GlobalMeshConfig.RPCPort,
	//	Handler: http.TimeoutHandler(
	//		router,
	//		time.Duration(app.GlobalMeshConfig.ClientRPCTimeout)*time.Millisecond,
	//		"server Timeout Handling Request",
	//	),
	//}

	go catchSignal()

	logger.Info(
		fmt.Sprintf(
			"start mesh for %d servicer in %d nodes",
			totalServicers,
			totalNodes,
		),
	)

	go func() {
		if err := fiberApp.Listen(":" + app.GlobalMeshConfig.RPCPort); err != nil && err != http.ErrServerClosed {
			log2.Fatal(err)
		}
	}()

	select {
	case <-ctx.Done():
		// Shutdown the server when the context is canceled
		logger.Info("bye bye! bip bop!")
	}
}
