package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modfin/clix"
	"github.com/modfin/creek/internal/config"
	"github.com/modfin/creek/internal/dao"
	"github.com/modfin/creek/internal/metrics"
	"github.com/modfin/creek/internal/mq"
	"github.com/modfin/creek/internal/utils"
	"github.com/modfin/henry/chanz"

	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		cancel()
		<-time.After(10 * time.Second)
		os.Exit(1)
	}()

	cmd := &cli.Command{
		Name: "creek producer",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL")},
			&cli.StringFlag{Name: "pg-uri", Sources: cli.EnvVars("PG_URI")},
			&cli.StringFlag{Name: "pg-publication-name", Sources: cli.EnvVars("PG_PUBLICATION_NAME")},
			&cli.StringFlag{Name: "pg-publication-slot", Sources: cli.EnvVars("PG_PUBLICATION_SLOT")},
			&cli.DurationFlag{Name: "pg-message-timeout", Value: time.Second * 10, Sources: cli.EnvVars("PG_MESSAGE_TIMEOUT")},
			&cli.StringSliceFlag{Name: "tables", Sources: cli.EnvVars("PG_TABLES")},
			&cli.StringFlag{Name: "nats-uri", Sources: cli.EnvVars("NATS_URI")},
			&cli.DurationFlag{Name: "nats-timeout", Value: time.Second * 30, Sources: cli.EnvVars("NATS_TIMEOUT")},
			&cli.IntFlag{Name: "nats-max-pending", Value: 4000, Sources: cli.EnvVars("NATS_MAX_PENDING")},
			&cli.StringFlag{Name: "nats-namespace", Value: "CREEK", Sources: cli.EnvVars("NATS_NAMESPACE")},
			&cli.IntFlag{Name: "prometheus-port", Value: 7779, Sources: cli.EnvVars("PROMETHEUS_PORT")},
		},
		Action: serve,
	}

	cmd.SliceFlagSeparator = " "

	if err := cmd.Run(ctx, os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func serve(ctx context.Context, cmd *cli.Command) error {

	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	db, err := dao.New(ctx, *cfg)
	if err != nil {
		logrus.Panicln("failed to connect to create db: ", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, time.Second*2)
	err = db.Connect(connectCtx)
	cancel()
	if err != nil {
		logrus.Panicln("failed to connect to database: ", err)
	}

	if err != nil {
		logrus.Panicln("failed to initialize database: ", err)
	}

	queue, err := mq.New(ctx, cfg.NatsUri, cfg.NatsNameSpace, cfg.NatsMaxPending, db)
	if err != nil {
		logrus.Panicln("failed to initialize nats: ", err)
	}

	go metrics.Start(ctx, cfg.PrometheusPort)

	replication, err := db.StartReplication(cfg.PgTables, cfg.PgPublicationName, cfg.PgPublicationSlot)
	if err != nil {
		logrus.Panicln("failed to start replication", err)
	}

	ws := queue.StartWalStream(replication.Stream())
	ss := queue.StartSchemaStream(replication.SchemaStream())

	err = queue.StartSnapshotAPI()
	if err != nil {
		logrus.Panicln("failed to start snapshot api", err)
	}
	err = queue.StartSchemaAPI()
	if err != nil {
		logrus.Panicln("failed to start schema api", err)
	}

	// All streams done
	streamsDone := chanz.EveryDone(
		replication.Done(),
		ws.Done(),
		ss.Done(),
		queue.SnapsDone(),
		ctx.Done(),
	)

	for {
		select {
		case <-queue.Done():
			logrus.Info("shutdown creekd")
			os.Exit(0)
		case <-streamsDone:
			// Now we can close and wait until everything has published
			queue.Close()
		}
	}
}

func initAndVerifyConfig(cmd *cli.Command) (*config.Config, error) {
	cfg := clix.Parse[config.Config](clix.V3(cmd))
	ll, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		ll = logrus.InfoLevel
	}
	limitedWriter := utils.NewRateLimitedWriter(60, 10, ll)
	prometheusHook := metrics.MustNewPrometheusHook()
	logrus.SetLevel(ll)
	logrus.AddHook(prometheusHook)
	logrus.AddHook(limitedWriter) // Writes to stdout with rate limit
	logrus.SetOutput(io.Discard)  // Discard messages

	if cfg.PgUri == "" {
		return nil, errors.New("pg-uri is required")
	}
	if cfg.PgPublicationName == "" {
		return nil, errors.New("pg-publication-name is required")
	}
	if cfg.PgPublicationSlot == "" {
		return nil, errors.New("pg-publication-slot is required")
	}
	return &cfg, nil
}
