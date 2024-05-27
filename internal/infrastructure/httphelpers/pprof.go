package httphelpers

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/logger"
)

func ListenAndServePprof(log logger.Logger, address string, port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/heap", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cpu", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/threadcreate", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/goroutine", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/block", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/mutex", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/allocs", pprof.Index)

	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	listenAddr := fmt.Sprintf("%s:%d", address, port)
	log.Warnf("Listening for DEBUG traffic on %s", listenAddr)
	return http.ListenAndServe(listenAddr, mux)
}
