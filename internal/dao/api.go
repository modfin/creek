package dao

import (
	"context"
	"errors"
	"strings"

	"github.com/sirupsen/logrus"
)

func (db *DB) startApi() {
	conn, err := db.pool.Acquire(db.ctx)
	if err != nil {
		logrus.Errorf("failed to aquire db connection: %v", err)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(db.ctx, "listen creek")
	if err != nil {
		logrus.Errorf("failed to listen to creek_consumer: %v", err)
	}
	logrus.Debug("listening for notifications on creek")

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
			logrus.Errorf("failed while waiting to get notification for creek_consumer: %v", err)
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
		}

	}
}
