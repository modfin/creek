package integration_tests

import (
	"context"
	"sync"
	"time"

	"github.com/modfin/creek"
	"github.com/modfin/creek/internal/config"
	"github.com/modfin/creek/internal/dao"
	"github.com/modfin/creek/internal/mq"
	"github.com/modfin/henry/chanz"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

var cl *creek.Client
var clientOnce sync.Once

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
			Uri:       GetNATSURL(),
			Timeout:   time.Second * 10,
			NameSpace: "CREEK",
			Retention: config.RetentionConfig{
				Policy:   jetstream.LimitsPolicy,
				MaxAge:   0,
				MaxBytes: 0,
				MaxMsgs:  0,
			},
		},
	}

}

func EnsureStarted(ctx context.Context) (*mq.MQ, <-chan struct{}) {
	cfg := GetConfig()

	msgCounter = &LogMsgCounter{msgs: 0}

	logrus.AddHook(msgCounter)

	db, err := dao.New(ctx, cfg)
	if err != nil {
		logrus.Fatal("failed to initialize database: ", err)
	}

	err = db.Connect(ctx)
	if err != nil {
		logrus.Fatal("failed to connect to database: ", err)
	}

	queue, err := mq.New(ctx, cfg.NatsConfig.Uri, cfg.NatsConfig.NameSpace, db)
	if err != nil {
		logrus.Fatal("failed to initialize nats: ", err)
	}

	replication, err := db.StartReplication(cfg.Tables, cfg.PgConfig.PublicationName, cfg.PgConfig.PublicationSlot)
	if err != nil {
		logrus.Fatal("failed to start replication", err)
	}

	ws := queue.StartWalStream(replication.Stream())
	ss := queue.StartSchemaStream(replication.SchemaStream())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = queue.ConsumeSchemaAPI()
				if err == nil {
					return
				}
				logrus.Errorf("failed to consume schema api: %v", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = queue.ConsumeSnapshotAPI()
				if err == nil {
					return
				}
				logrus.Errorf("failed to consume snapshot api: %v", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	return queue, chanz.EveryDone(
		replication.Done(),
		ws.Done(),
		ss.Done(),
		queue.SnapsDone(),
		ctx.Done(),
	)
}

func GetCreekConn() *creek.Client {
	clientOnce.Do(func() {
		cfg := GetConfig()
		cl = creek.NewClient(cfg.NatsConfig.Uri, cfg.NatsConfig.NameSpace, DBname)
	})

	return cl
}

func TimeoutContext(timeout time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	return ctx
}
