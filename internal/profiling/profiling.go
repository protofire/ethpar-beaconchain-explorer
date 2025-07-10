package profiling

import (
	"fmt"
	"net/http"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"

	_ "net/http/pprof"
)

var log = logger.New(nil).WithField("module", "profiling")

func StartProfiling(enabled bool, address string, port uint16) {
	if enabled {
		go func() {
			addr := fmt.Sprintf("%s:%d", address, port)
			log.Infof("starting pprof http server on %s", addr)

			// Gracefully handle errors
			if err := http.ListenAndServe(addr, nil); err != nil {
				log.Errorf("pprof server error: %v", err)
			}
		}()
	}
}