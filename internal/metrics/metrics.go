package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"

	"github.com/modfin/creek"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var pgReads = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "creek_producer_pg_reads",
	Help: "total numbers of row into postgres from creek streams",
}, []string{"creek_stream_type", "creek_producer_pg_op", "creek_pg_source"})

var behindTime = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "creek_producer_pg_behind_time",
	Help: "total time between postgres wal event and processing in milliseconds",
})

var behindBytes = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "creek_producer_pg_behind_bytes",
	Help: "total unprocessed bytes in wal log",
})

func init() {
	prometheus.MustRegister(pgReads)
	prometheus.MustRegister(behindTime)
	prometheus.MustRegister(behindBytes)
}

func Start(ctx context.Context, port int) {
	srv := &http.Server{Addr: fmt.Sprintf(":%d", port)}
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		<-ctx.Done()
		cc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logrus.Info("shutting down metrics server")
		srv.Shutdown(cc)
		logrus.Info("metrics server is shut down")
	}()

	logrus.Infof("starting metrics server on :%d", port)

	err := srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logrus.Error(err)
	}
}

func SetBehindLSN(last pglogrepl.LSN, lastProcessed pglogrepl.LSN) {
	diff := float64(last - lastProcessed)
	if diff < 0 {
		diff = 0
	}
	behindBytes.Set(diff)
}

func SetBehindTime(duration time.Duration) {
	behindTime.Set(float64(duration.Milliseconds()))
}

func IncRead(streamType creek.StreamType, op string, source string) {
	pgReads.
		With(map[string]string{
			"creek_stream_type":    string(streamType),
			"creek_pg_source":      source,
			"creek_producer_pg_op": op}).
		Inc()
}

// Inspired by https://github.com/weaveworks/promrus/blob/master/promrus.go

type PrometheusHook struct {
	counterVec *prometheus.CounterVec
}

var supportedLevels = []logrus.Level{logrus.DebugLevel, logrus.InfoLevel, logrus.WarnLevel, logrus.ErrorLevel}

func NewPrometheusHook() (*PrometheusHook, error) {
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "log_messages_total",
		Help: "Total number of log messages.",
	}, []string{"level"})
	// Initialise counters for all supported levels:
	for _, level := range supportedLevels {
		counterVec.WithLabelValues(level.String())
	}
	err := prometheus.Register(counterVec)
	if err != nil {
		return nil, err
	}
	return &PrometheusHook{
		counterVec: counterVec,
	}, nil
}

func MustNewPrometheusHook() *PrometheusHook {
	hook, err := NewPrometheusHook()
	if err != nil {
		panic(err)
	}
	return hook
}

func (hook *PrometheusHook) Fire(entry *logrus.Entry) error {
	hook.counterVec.WithLabelValues(entry.Level.String()).Inc()
	return nil
}

func (hook *PrometheusHook) Levels() []logrus.Level {
	return supportedLevels
}
