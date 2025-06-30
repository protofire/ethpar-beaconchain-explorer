package eth1indexer

import (
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// observeBlockMetrics exports timing metrics to Prometheus for execution-layer
// block retrieval operations: headers, receipts, traces, and total duration.
func observeBlockMetrics(timings *types.GetBlockTimings, duration time.Duration) {
	metrics.TaskDuration.WithLabelValues("bt_index_from_node").Observe(duration.Seconds())
	metrics.TaskDuration.WithLabelValues("rpc_el_get_block_headers").Observe(timings.Headers.Seconds())
	metrics.TaskDuration.WithLabelValues("rpc_el_get_block_receipts").Observe(timings.Receipts.Seconds())
	metrics.TaskDuration.WithLabelValues("rpc_el_get_block_traces").Observe(timings.Traces.Seconds())
}
