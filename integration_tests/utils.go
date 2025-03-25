package integration_tests

import (
	"context"
	"sync"
	"time"

	"github.com/modfin/creek"
	"github.com/modfin/creek/internal/config"
	"github.com/modfin/creek/internal/dao"
	"github.com/modfin/creek/internal/mq"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

var start sync.Once
var conn *creek.Conn
var connOnce sync.Once
var testCtx context.Context

type LogMsgCounter struct {
	msgs int
}

func (l *LogMsgCounter) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (l *LogMsgCounter) Fire(*logrus.Entry) error {
	l.msgs++
	return nil
}

func (l *LogMsgCounter) Reset() {
	l.msgs = 0
}

func (l *LogMsgCounter) Msgs() int {
	return l.msgs
}

var msgCounter *LogMsgCounter

func GetConfig() config.Config {

	return config.Config{
		LogLevel: "trace",
		PgConfig: config.PgConfig{
			Uri:             GetDBURL(),
			PublicationName: "test",
			PublicationSlot: "test_0",
			MessageTimeout:  time.Second * 10,
		},
		Tables: []string{"public.types_data", "public.other", "public.types", "public.prices"},
		NatsConfig: config.NatsConfig{
			Uri:        GetNATSURL(),
			Timeout:    time.Second * 10,
			MaxPending: 100,
			NameSpace:  "CREEK",
			Retention: config.RetentionConfig{
				Policy:   jetstream.LimitsPolicy,
				MaxAge:   0,
				MaxBytes: 0,
				MaxMsgs:  0,
			},
		},
	}

}

func EnsureStarted() {
	start.Do(func() {
		cfg := GetConfig()

		msgCounter = &LogMsgCounter{msgs: 0}

		logrus.AddHook(msgCounter)

		db, err := dao.New(testCtx, cfg)
		if err != nil {
			logrus.Fatal("failed to initialize database: ", err)
		}

		err = db.Connect(testCtx)
		if err != nil {
			logrus.Fatal("failed to connect to database: ", err)
		}

		queue, err := mq.New(testCtx, cfg.NatsConfig.Uri, cfg.NatsConfig.NameSpace, cfg.NatsConfig.MaxPending, db)
		if err != nil {
			logrus.Fatal("failed to initialize nats: ", err)
		}

		replication, err := db.StartReplication(cfg.Tables, cfg.PgConfig.PublicationName, cfg.PgConfig.PublicationSlot)
		if err != nil {
			logrus.Fatal("failed to start replication", err)
		}

		queue.StartWalStream(replication.Stream())
		queue.StartSchemaStream(replication.SchemaStream())
		err = queue.StartSnapshotAPI()
		if err != nil {
			logrus.Fatal("failed to start snapshot api", err)
		}
		err = queue.StartSchemaAPI()
		if err != nil {
			logrus.Fatal("failed to start schema api", err)
		}
	})
}

func GetCreekConn() *creek.Conn {
	connOnce.Do(func() {
		cfg := GetConfig()

		var err error
		conn, err = creek.NewClient(cfg.NatsConfig.Uri, cfg.NatsConfig.NameSpace, DBname).Connect(context.Background())
		if err != nil {
			logrus.Fatal("failed to connect client", err)
		}
	})

	return conn
}

func TimeoutContext(timeout time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return ctx
}
