package integration_tests

import (
	"context"
	"sync"
	"time"

	"github.com/modfin/creek"
	"github.com/modfin/creek/internal/config"
	"github.com/modfin/creek/internal/dao"
	"github.com/modfin/creek/internal/mq"
	"github.com/sirupsen/logrus"
)

var start sync.Once
var conn *creek.Conn
var connOnce sync.Once
var testCtx context.Context

func GetConfig() config.Config {

	return config.Config{
		LogLevel:          "trace",
		PgUri:             GetDBURL(),
		PgPublicationName: "test",
		PgPublicationSlot: "test_0",
		PgMessageTimeout:  time.Second * 10,
		PgTables:          []string{"public.types_data", "public.other", "public.types", "public.prices"},
		NatsUri:           GetNATSURL(),
		NatsTimeout:       time.Second * 10,
		NatsMaxPending:    100,
		NatsNameSpace:     "CREEK",
	}

}

func EnsureStarted() {
	start.Do(func() {
		cfg := GetConfig()

		db, err := dao.New(testCtx, cfg)
		if err != nil {
			logrus.Fatal("failed to initialize database: ", err)
		}

		err = db.Connect(testCtx)
		if err != nil {
			logrus.Fatal("failed to connect to database: ", err)
		}

		queue, err := mq.New(testCtx, cfg.NatsUri, cfg.NatsNameSpace, cfg.NatsMaxPending, db)
		if err != nil {
			logrus.Fatal("failed to initialize nats: ", err)
		}

		replication, err := db.StartReplication(cfg.PgTables, cfg.PgPublicationName, cfg.PgPublicationSlot)
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
		conn, err = creek.NewClient(cfg.NatsUri, cfg.NatsNameSpace).Connect()
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
