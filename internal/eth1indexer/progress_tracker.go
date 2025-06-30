package eth1indexer

import (
	"sync/atomic"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
)

// progressTracker tracks progress metrics and logs periodic updates
// about block indexing, including rate, estimated time remaining, and percentage complete.
type progressTracker struct {
	start       int64
	end         int64
	startTime   time.Time
	lastLogTime time.Time
	total       int64
	logger      *logger.Logger
	logInterval time.Duration
}

// newProgressTracker initializes and returns a new progressTracker
// to monitor progress between the given start and end block numbers.
func newProgressTracker(start, end int64, logger *logger.Logger) *progressTracker {
	return &progressTracker{
		start:       start,
		end:         end,
		startTime:   time.Now(),
		lastLogTime: time.Now(),
		logger:      logger,
		logInterval: 5 * time.Second,
	}
}

// Tick updates the internal block counter and logs current progress,
// including throughput, percent completion, and estimated time to finish.
func (p *progressTracker) Tick(currentBlock int64) {
	total := atomic.AddInt64(&p.total, 1)

	if time.Since(p.lastLogTime) < p.logInterval {
		return
	}

	elapsed := time.Since(p.startTime).Seconds()
	speed := float64(total) / elapsed
	progress := float64(currentBlock-p.start) / float64(p.end-p.start) * 100
	eta := time.Duration(float64(p.end-currentBlock)/speed) * time.Second

	p.logger.Infof("Indexed block %d of %d (%.1f%%) — %.2f blk/sec — ETA: %s",
		currentBlock, p.end, progress, speed, eta)

	p.lastLogTime = time.Now()
}
