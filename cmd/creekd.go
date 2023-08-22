package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modfin/creek/internal/config"
	"github.com/modfin/creek/internal/dao"
	"github.com/modfin/creek/internal/metrics"
	"github.com/modfin/creek/internal/mq"
	"github.com/modfin/henry/chanz"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		cancel()
		<-time.After(2 * time.Second)
		os.Exit(1)
	}()

	var conf config.Config
	var strSlice = cli.NewStringSlice()

	app := &cli.App{
		Name: "creek producer",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "log-level", Value: "info", EnvVars: []string{"LOG_LEVEL"}, Destination: &conf.LogLevel},
			&cli.StringFlag{Name: "pg-uri", EnvVars: []string{"PG_URI"}, Destination: &conf.PgUri},
			&cli.StringFlag{Name: "pg-publication-name", EnvVars: []string{"PG_PUBLICATION_NAME"}, Destination: &conf.PgPublicationName},
			&cli.StringFlag{Name: "pg-publication-slot", EnvVars: []string{"PG_PUBLICATION_SLOT"}, Destination: &conf.PgPublicationSlot},
			&cli.DurationFlag{Name: "pg-message-timeout", Value: time.Second * 10, EnvVars: []string{"PG_MESSAGE_TIMEOUT"}, Destination: &conf.PgMessageTimeout},
			&cli.StringSliceFlag{Name: "tables", EnvVars: []string{"PG_TABLES"}, Destination: strSlice},
			&cli.StringFlag{Name: "nats-uri", EnvVars: []string{"NATS_URI"}, Destination: &conf.NatsUri},
			&cli.DurationFlag{Name: "nats-timeout", Value: time.Second * 30, EnvVars: []string{"NATS_TIMEOUT"}, Destination: &conf.NatsTimeout},
			&cli.IntFlag{Name: "nats-max-pending", Value: 4000, EnvVars: []string{"NATS_MAX_PENDING"}, Destination: &conf.NatsMaxPending},
			&cli.StringFlag{Name: "nats-namespace", Value: "CREEK", EnvVars: []string{"NATS_NAMESPACE"}, Destination: &conf.NatsNameSpace},
			&cli.IntFlag{Name: "prometheus-port", Value: 7779, EnvVars: []string{"PROMETHEUS_PORT"}, Destination: &conf.PrometheusPort},
		},
		Before: func(c *cli.Context) error {
			conf.PgTables = strSlice.Value()
			config.Set(conf)
			if conf.PgUri == "" {
				return errors.New("pg-uri is required")
			}
			if conf.PgPublicationName == "" {
				return errors.New("pg-publication-name is required")
			}
			if conf.PgPublicationSlot == "" {
				return errors.New("pg-publication-slot is required")
			}

			return nil
		},
		Action: serve(ctx),
	}

	app.SliceFlagSeparator = " "

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func serve(ctx context.Context) cli.ActionFunc {
	return func(c *cli.Context) error {
		cfg := config.Get()

		db, err := dao.New(ctx, cfg)
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
}
