package dao

import (
	"context"
	"errors"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modfin/creek/internal/metrics"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

func (db *DB) startApi() {
	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 15 * time.Second
	b.MaxElapsedTime = 1<<63 - 1

	connect := func() (*pgxpool.Conn, error) {
		conn, err := db.pool.Acquire(db.ctx)
		if err != nil {
			return nil, err
		}
		_, err = conn.Exec(db.ctx, "listen creek")
		if err != nil {
			conn.Release()
		}
		return conn, err
	}

	notify := func(err error, timeout time.Duration) {
		logrus.Errorf("[listen api] failed to connect to database, retrying in %ds", int(timeout.Seconds()))
	}

	tryConnect := func() (conn *pgxpool.Conn, err error) {
		b = backoff.NewExponentialBackOff()
		b.MaxInterval = 15 * time.Second
		b.MaxElapsedTime = 1<<63 - 1

		conn, err = backoff.RetryNotifyWithData(connect, b, notify)
		return
	}

	conn, err := tryConnect()
	if err != nil {
		logrus.Errorf("[listen api] failed to connect: %v", err)
		return
	}
	defer conn.Release()

	for {
		select {
		case <-db.ctx.Done():
			return
		default:
		}
		notification, err := conn.Conn().WaitForNotification(db.ctx)
		if errors.Is(err, context.Canceled) {
			continue
		}
		if err != nil {
			logrus.Errorf("failed while waiting to get notification for _creek: %v", err)
			if err.Error() == "conn closed" {
				conn, err = tryConnect() // Blocks until new connection
				if err != nil {
					logrus.Errorf("[listen api] failed to reconnect: %v", err)
					return
				}
			}
			continue
		}

		logrus.Debug("received command:", notification.Payload)

		split := strings.SplitN(notification.Payload, " ", 2)
		if len(split) != 2 {
			logrus.Errorf("recieved unknown notification %s, skipping", notification.Payload)
			continue
		}

		if split[0] == "ADD" {
			err = db.initRelationSchema(split[1])
			if err != nil {
				logrus.Errorf("failed to save initial schema for table %s: %v", split[1], err)
			}
			metrics.IncSubscribedTables()
		}

		if split[0] == "REMOVE" {
			metrics.DecSubscribedTables()
		}

	}
}
