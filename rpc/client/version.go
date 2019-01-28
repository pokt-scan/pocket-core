package client

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/pokt-network/pocket-core/const"
	"github.com/pokt-network/pocket-core/rpc/shared"
)

// "getClientAPIVersion" handles the localhost:<client-port>/v1 call.
func GetClientAPIVersion(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	shared.WriteResponse(w, _const.RAPIVERSION)
}
