package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/modfin/creek"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var pgReads = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "creek_producer_pg_reads",
	Help: "total numbers of row into postgres from creek streams",
}, []string{"creek_stream_type", "creek_producer_pg_op", "creek_pg_source"})

func init() {
	prometheus.MustRegister(pgReads)
}

func Start(ctx context.Context, port int) {
	e := echo.New()
	e.Use(echoprometheus.NewMiddleware("creek_producer")) // adds middleware to gather metrics
	e.GET("/metrics", echoprometheus.NewHandler())

	go func() {
		<-ctx.Done()
		cc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logrus.Info("shutting down metrics server")
		_ = e.Shutdown(cc)
		logrus.Info("metrics server is shut down")
	}()

	err := e.Start(fmt.Sprintf(":%d", port))
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logrus.Fatal(err)
	}
}

func IncRead(streamType creek.StreamType, op string, source string) {
	pgReads.
		With(map[string]string{
			"creek_stream_type":    string(streamType),
			"creek_pg_source":      source,
			"creek_producer_pg_op": op}).
		Inc()
}
