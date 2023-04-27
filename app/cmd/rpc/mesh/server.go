package mesh

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/akrylysov/pogreb"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/pokt-network/pocket-core/app"
	pocketTypes "github.com/pokt-network/pocket-core/x/pocketcore/types"
	"github.com/robfig/cron/v3"
	"github.com/valyala/fasthttp"
	"io"
	log2 "log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Route struct {
	Name        string
	Method      string
	Path        string
	Auth        bool
	HandlerFunc fiber.Handler
}

type Routes []Route

type transport struct {
	http.RoundTripper
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// RoundTrip - handle http requests before/after they run and hook to response handlers bases on path.
func (t *transport) RoundTrip(r *http.Request) (w *http.Response, err error) {
	w, err = t.RoundTripper.RoundTrip(r)
	if err != nil {
		return nil, err
	}

	if w.StatusCode != 200 {
		return w, nil
	}

	rr, err := NewReusableReader(w.Body)
	if err != nil {
		return nil, err
	}

	w.Body = io.NopCloser(rr)

	return w, nil
}

type reusableReader struct {
	io.Reader
	readBuf *bytes.Buffer
	backBuf *bytes.Buffer
}

// Read - read the buffer and reset to allow multiple reads
func (r reusableReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.reset()
	}
	return n, err
}

// reset - reset buffer to allow other reads
func (r reusableReader) reset() {
	_, _ = io.Copy(r.readBuf, r.backBuf)
}

// NewReusableReader - create new Reader that allow to be read multiple times.
func NewReusableReader(r io.Reader) (io.Reader, error) {
	readBuf := bytes.Buffer{}
	_, err := readBuf.ReadFrom(r)
	if err != nil {
		return nil, err
	} // error handling ignored for brevity
	backBuf := bytes.Buffer{}

	return reusableReader{
		io.TeeReader(&readBuf, &backBuf),
		&readBuf,
		&backBuf,
	}, nil
}

// retryRelaysPolicy - evaluate requests to understand if should or not retry depending on the servicer code response.
func retryRelaysPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if err != nil {
		logger.Error(
			fmt.Sprintf(
				"error dispatching relay to servicer: %s",
				err.Error(),
			),
		)
		return true, nil
	}

	servicerAddress := resp.Request.Header.Get(ServicerHeader)

	if resp.StatusCode != 200 {
		if resp.StatusCode >= 401 {
			// 401+ could be fixed between restart and reload of cache.
			// 5xx mean something go wrong on servicer node and after a restart could be fixed?
			return true, nil
		}

		if !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
			return true, nil
		}

		result := RPCRelayResponse{}
		err = json.NewDecoder(resp.Body).Decode(&result)

		if err != nil {
			logger.Error(
				fmt.Sprintf(
					"error decoding servicer %s relay response: %s",
					servicerAddress,
					err.Error(),
				),
			)
			return true, err
		}

		ctxResult := ctx.Value("result").(*RPCRelayResponse)
		ctxResult.Success = result.Success
		ctxResult.Dispatch = result.Dispatch
		ctxResult.Error = result.Error

		if ctxResult.Error.Code == pocketTypes.CodeDuplicateProofError {
			return false, nil
		}

		return !IsInvalidRelayCode(result.Error.Code), nil
	}

	return false, nil
}

// ProxyRequest - proxy request to ServicerURL
func ProxyRequest(c *fiber.Ctx) error {
	target := fmt.Sprintf("%s%s", GetRandomNode().URL, c.Path())
	if err := proxy.Do(c, target); err != nil {
		return err
	}
	// Remove Server header from response
	c.Response().Header.Del(fiber.HeaderServer)
	return nil
}

// IsAuthorized - Fiber Middleware to validate authorization header
func IsAuthorized(c *fiber.Ctx) error {
	token := c.GetReqHeaders()[AuthorizationHeader]

	if token == "" {
		// fallback support
		token = c.Query("authtoken")
	}

	if token != GetAuthToken() {
		return fiber.NewError(401, "Unauthorized")
	}

	// continue
	return c.Next()
}

// WriteResponse - Fiber success response sugar function
func WriteResponse(body interface{}, c *fiber.Ctx) error {
	if e := c.JSON(body); e != nil {
		return WriteError(500, e.Error(), c)
	}
	c.Set("Content-type", "application/json; charset=utf-8")
	c.Status(200)
	return nil
}

// WriteError - Fiber error response sugar function
func WriteError(code int, message string, c *fiber.Ctx) error {
	e := c.JSON(&rpcError{
		Code:    code,
		Message: message,
	})

	if e != nil {
		return fiber.NewError(500, e.Error())
	}

	c.Set("Content-type", "application/json; charset=utf-8")

	return nil
}

// prepareHttpClients - prepare http clients & transports
func prepareHttpClients() {
	logger.Info("initializing http clients")
	servicerTransport := http.DefaultTransport.(*http.Transport).Clone()
	servicerTransport.MaxIdleConns = app.GlobalMeshConfig.ServicerRPCMaxIdleConnections
	servicerTransport.MaxConnsPerHost = app.GlobalMeshConfig.ServicerRPCMaxConnsPerHost
	servicerTransport.MaxIdleConnsPerHost = app.GlobalMeshConfig.ServicerRPCMaxIdleConnsPerHost

	maxIdleConnDuration, _ := time.ParseDuration("1h")

	chainsClient = &fasthttp.Client{
		ReadTimeout:                   time.Duration(app.GlobalMeshConfig.ChainRPCReadTimeout) * time.Millisecond,
		WriteTimeout:                  time.Duration(app.GlobalMeshConfig.ChainRPCWriteTimeout) * time.Millisecond,
		MaxIdleConnDuration:           maxIdleConnDuration,
		NoDefaultUserAgentHeader:      true, // Don't send: User-Agent: fasthttp
		DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this
		DisablePathNormalizing:        false,
		MaxConnsPerHost:               app.GlobalMeshConfig.ChainRPCMaxConnsPerHost,
		// increase DNS cache time to an hour instead of default minute
		Dial: (&fasthttp.TCPDialer{
			Concurrency:      4096,
			DNSCacheDuration: time.Hour,
		}).Dial,
	}
	servicerClient = &fasthttp.Client{
		ReadTimeout:                   time.Duration(app.GlobalMeshConfig.ServicerRPCReadTimeout) * time.Millisecond,
		WriteTimeout:                  time.Duration(app.GlobalMeshConfig.ServicerRPCWriteTimeout) * time.Millisecond,
		MaxIdleConnDuration:           maxIdleConnDuration,
		MaxConnsPerHost:               app.GlobalMeshConfig.ServicerRPCMaxConnsPerHost,
		NoDefaultUserAgentHeader:      true, // Don't send: User-Agent: fasthttp
		DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this
		DisablePathNormalizing:        false,
		// increase DNS cache time to an hour instead of default minute
		Dial: (&fasthttp.TCPDialer{
			Concurrency:      4096,
			DNSCacheDuration: time.Hour,
		}).Dial,
	}

	relaysClient = retryablehttp.NewClient()
	relaysClient.RetryMax = app.GlobalMeshConfig.ServicerRetryMaxTimes
	relaysClient.HTTPClient = &http.Client{
		Timeout:   time.Duration(app.GlobalMeshConfig.ServicerRPCTimeout) * time.Millisecond,
		Transport: servicerTransport,
	}
	relaysClient.Logger = &LevelHTTPLogger{}
	relaysClient.RetryWaitMin = time.Duration(app.GlobalMeshConfig.ServicerRetryWaitMin) * time.Millisecond
	relaysClient.RetryWaitMax = time.Duration(app.GlobalMeshConfig.ServicerRetryWaitMax) * time.Millisecond
	relaysClient.CheckRetry = retryRelaysPolicy
}

// catchSignal - catch system signals and process them
func catchSignal() {
	terminateSignals := make(chan os.Signal, 1)
	reloadSignals := make(chan os.Signal, 1)

	signal.Notify(terminateSignals, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM, os.Kill, os.Interrupt) //NOTE:: syscall.SIGKILL we cannot catch kill -9 as its force kill signal.

	signal.Notify(reloadSignals, syscall.SIGUSR1)

	for { // We are looping here because config reload can happen multiple times.
		select {
		case s := <-terminateSignals:
			logger.Info("shutting down server gracefully, SIGNAL NAME:", s)
			StopRPC()
			finish()
			break // break is not necessary to add here as if server is closed our main function will end.
		case s := <-reloadSignals:
			logger.Debug("reloading SIGNAL received:", s)
			reloadChains()
			reloadServicers()
		}
	}
}

// initCache - initialize cache
func initCache() {
	var err error

	logger.Info("initializing relays cache")
	relaysCacheFilePath := app.GlobalMeshConfig.DataDir + app.FS + app.GlobalMeshConfig.RelayCacheFile
	relaysCacheDb, err = pogreb.Open(relaysCacheFilePath, &pogreb.Options{
		// BackgroundSyncInterval sets the amount of time between background Sync() calls.
		//
		// Setting the value to 0 disables the automatic background synchronization.
		// Setting the value to -1 makes the DB call Sync() after every write operation.
		BackgroundSyncInterval: time.Duration(app.GlobalMeshConfig.RelayCacheBackgroundSyncInterval) * time.Millisecond,
		// BackgroundCompactionInterval sets the amount of time between background Compact() calls.
		//
		// Setting the value to 0 disables the automatic background compaction.
		BackgroundCompactionInterval: time.Duration(app.GlobalMeshConfig.RelayCacheBackgroundCompactionInterval) * time.Millisecond,
	})
	if err != nil {
		log2.Fatal(err)
		return
	}

	logger.Info(fmt.Sprintf("resuming %d relays from cache", relaysCacheDb.Count()))
	it := relaysCacheDb.Items()
	for {
		key, val, err := it.Next()
		if err == pogreb.ErrIterationDone {
			break
		}
		if err != nil {
			log2.Fatal(err)
		}

		logger.Debug("loading relay hash=%s", hex.EncodeToString(key))
		relay := decodeCacheRelay(val)

		if relay != nil {
			servicerAddress, err := GetAddressFromPubKeyAsString(relay.Proof.ServicerPubKey)
			if err != nil {
				logger.Debug(
					fmt.Sprintf(
						"removing relay hash=%s from cache because was unable decode pk from pk file",
						relay.RequestHashString(),
					),
				)
				deleteCacheRelay(relay)
				continue
			}

			servicerNode, ok := servicerMap.Load(servicerAddress)
			if !ok {
				logger.Debug(
					fmt.Sprintf(
						"removing relay hash=%s from cache because was unable to load servicer %s from pk file",
						relay.RequestHashString(),
						hex.EncodeToString(key),
					),
				)
				deleteCacheRelay(relay)
				continue
			}

			if !ok {
				logger.Debug(
					fmt.Sprintf(
						"removing relay hash=%s from cache because was unable to cast *servicer instance for %s",
						relay.RequestHashString(),
						hex.EncodeToString(key),
					),
				)
				deleteCacheRelay(relay)
				continue
			}

			servicerNode.Node.Worker.Submit(func() {
				notifyServicer(relay)
			})
		}
	}
}

// initCrons - initialize in memory cron jobs
func initCrons() {
	// start cron for height pooling
	cronJobs = cron.New()

	logger.Info("initializing session cache clean up")
	// schedule clean old session job
	cleanOldSessions(cronJobs)

	// start all the cron jobs
	cronJobs.Start()
}
