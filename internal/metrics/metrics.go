package metrics

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/version"
)

var (
	Version                *prometheus.GaugeVec
	HttpRequestsTotal      *prometheus.CounterVec
	HttpRequestsInFlight   *prometheus.GaugeVec
	HttpRequestsDuration   *prometheus.HistogramVec
	Tasks                  *prometheus.CounterVec
	TaskDuration           *prometheus.HistogramVec
	DBSLongRunningQueries  *prometheus.CounterVec
	Errors                 *prometheus.CounterVec
	NotificationsCollected *prometheus.CounterVec
	NotificationsQueued    *prometheus.CounterVec
	NotificationsSent      *prometheus.CounterVec
	Counter                *prometheus.CounterVec
)

var (
	log  = logger.New(nil).WithField("module", "metrics")
	once sync.Once
)

// Init creates & registers all collectors exactly once.
// Pass nil to use prometheus.DefaultRegisterer / DefaultGatherer.
func Init(reg prometheus.Registerer) {
	once.Do(func() {
		if reg == nil {
			reg = prometheus.DefaultRegisterer
		}
		const ns = "" // no namespace; add one if you run multiple components

		Version = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "version",
			Help:      "Gauge with version string in label",
		}, []string{"version"})
		Version.WithLabelValues(version.Version).Set(1)

		HttpRequestsTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "http_requests_total",
			Help:      "Total HTTP requests by path, method and status",
		}, []string{"path", "method", "status_code"})

		HttpRequestsInFlight = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Name:      "http_requests_in_flight",
			Help:      "Current HTTP requests being served",
		}, []string{"path", "method"})

		HttpRequestsDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Name:      "http_requests_duration_seconds",
			Help:      "HTTP request duration in seconds",
		}, []string{"path", "method"})

		Tasks = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "tasks_total",
			Help:      "Number of finished tasks by name",
		}, []string{"name"})

		TaskDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns,
			Name:      "task_duration_seconds",
			Help:      "Task duration",
			Buckets:   []float64{.05, .1, .5, 1, 5, 10, 20, 60, 90, 120, 180, 300},
		}, []string{"task"})

		DBSLongRunningQueries = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "db_long_running_queries_total",
			Help:      "Long-running queries by database and statement",
		}, []string{"database", "query"})

		Errors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "errors_total",
			Help:      "Application errors by name",
		}, []string{"name"})

		NotificationsCollected = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "notifications_collected_total",
			Help:      "Notifications collected by event type",
		}, []string{"event_type"})

		NotificationsQueued = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "notifications_queued_total",
			Help:      "Notifications queued by channel and event type",
		}, []string{"channel", "event_type"})

		NotificationsSent = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "notifications_sent_total",
			Help:      "Notifications sent by channel and status",
		}, []string{"channel", "status"})

		Counter = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: ns,
			Name:      "counter_total",
			Help:      "Generic counter by name",
		}, []string{"name"})
	})
}

// StartHTTP spins up `/metrics` on addr.  It returns the *http.Server so callers
// can shut it down if needed.
func StartHTTP(ctx context.Context, addr string, gatherer prometheus.Gatherer) (*http.Server, error) {
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html>
<head><title>prometheus-metrics</title></head>
<body>
<h1>prometheus-metrics</h1>
<p><a href='/metrics'>metrics</a></p>
</body>
</html>`))
	}))

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Infof("serving metrics on %s", addr)
	return srv, srv.ListenAndServe()
}

// MonitorDB bumps a counter for every query that has run > 1 min.
// Call this in its own goroutine and cancel via ctx.
func MonitorDB(ctx context.Context, db *sqlx.DB) {
	re := regexp.MustCompile(`[\t\r\n\s{2,}]+`)
	t := time.NewTicker(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var rows []struct {
				Datname  sql.NullString `db:"datname"`
				Duration sql.NullFloat64
				Query    sql.NullString
			}
			err := db.Select(&rows, `
				select
					datname,
					extract(epoch from clock_timestamp()) - extract(epoch from query_start) as duration,
					query
				from pg_stat_activity
				where query != '<IDLE>'
				  and query not ilike '%pg_stat_activity%'
				  and query_start is not null
				  and state = 'active'
				  and age(clock_timestamp(), query_start) >= interval '1 minutes'`)
			if err != nil {
				log.WithError(err).Error("monitor-db failed")
				continue
			}

			for _, q := range rows {
				norm := re.ReplaceAllString(strings.TrimSpace(q.Query.String), " ")
				DBSLongRunningQueries.WithLabelValues(q.Datname.String, norm).Inc()
			}
		}
	}
}

// Middleware that records request metrics.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		route := mux.CurrentRoute(r)
		path, err := route.GetPathTemplate()
		if err != nil {
			path = "UNDEFINED"
		}
		method := strings.ToUpper(r.Method)

		HttpRequestsInFlight.WithLabelValues(path, method).Inc()
		defer HttpRequestsInFlight.WithLabelValues(path, method).Dec()

		delegator := &responseWriter{ResponseWriter: w}
		next.ServeHTTP(delegator, r)

		status := strconv.Itoa(delegator.status)
		HttpRequestsTotal.WithLabelValues(path, method, status).Inc()
		HttpRequestsDuration.WithLabelValues(path, method).
			Observe(time.Since(start).Seconds())
	})
}

type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.wroteHeader = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}

func StartMetrics(enabled bool, address string) {
	if enabled {
		Init(nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if _, err := StartHTTP(ctx, address, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
}