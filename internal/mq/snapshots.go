package mq

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/modfin/creek"
	"github.com/modfin/creek/internal/dao"

	"github.com/hamba/avro/v2"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func (mq *MQ) ConsumeSnapshotAPI() error {
	logrus.Infof("creating connection on %s for snapshot requests", mq.streamName(creek.SnapStream))
	conn, err := mq.getConnection()
	if err != nil {
		return fmt.Errorf("could not get connection, err %v", err)
	}
	defer conn.Close()
	logrus.Infof("listening on %s for snapshot requests", mq.streamName(creek.SnapStream))
	// Expect messages to be in json containing, example:
	// {"database": "db", "namespace": "public", "table": "data"}
	sub, err := conn.SubscribeSync(mq.streamName(creek.SnapStream))

	for {
		select {
		case <-mq.ctx.Done():
			return nil
		default:
			m, err := sub.NextMsg(10 * time.Second)
			if err != nil && errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if err != nil {
				return fmt.Errorf("fetch next message: %w", err)
			}
			err = mq.handleSnapShotMessage(m)
			if err != nil {
				return err
			}
			err = m.Ack()
			if err != nil {
				return fmt.Errorf("ack message: %w", err)
			}
		}

	}
}

func (mq *MQ) handleSnapShotMessage(m *nats.Msg) error {
	if m.Reply == "" {
		return nil
	}

	var received creek.SnapshotRequest
	err := json.Unmarshal(m.Data, &received)
	if err != nil {
		logrus.Errorf("[snapshot] failed to read json message for snapshot: %v", err)
		return err
	}

	ok, err := mq.db.CanSnapshot(received.Namespace, received.Table)
	if err != nil {
		logrus.Errorf("[snapshot] failed to check if snapshot allowed: %v", err)
		return err
	}
	if !ok {
		return fmt.Errorf("snapshot not allowed")
	}

	header, reader, err := mq.db.Snapshot(received.Namespace, received.Table)
	if err != nil {
		logrus.Errorf("[snapshot] failed to start snapshot for %s.%s", received.Namespace, received.Table)
		return err
	}

	returnTopic := mq.genSnapTopic(int64(time.Now().Nanosecond()), time.Now(), received)

	// Respond with topic to client
	err = m.Respond([]byte(returnTopic))
	if err != nil {
		logrus.Errorf("[snapshot] failed to return snapshot topic for %s.%s", received.Namespace, received.Table)
		return err
	}

	go mq.streamSnapshots(header, reader, returnTopic)
	return nil
}

func (mq *MQ) streamSnapshots(header creek.SnapshotHeader, reader *dao.SnapshotReader, topic string) {
	mq.snapWg.Add(1)
	defer mq.snapWg.Done()

	// Publish header
	b, err := json.Marshal(header)
	if err != nil {
		logrus.Errorf("failed to marshal snapshot header for topic %s. Ending snapshot. %v", topic, err)
		return
	}

	mq.snapshotBus <- msg{
		subject:    topic,
		data:       b,
		identifier: header.LSN,
	}

	i := 0
	for row := range reader.Rows() {
		i += 1

		b, err := avro.Marshal(reader.Schema(), row)
		if err != nil {
			// TODO: Should we push this metrics or notify snap caller about failed rows?
			logrus.Errorf("failed to marshal snapshot data for topic %s, skipping message. err: %v", topic, err)
			continue
		}

		mq.snapshotBus <- msg{
			subject:    topic,
			data:       b,
			identifier: fmt.Sprintf("%s-row-%d", header.LSN, i),
		}
	}

	// Snapshot failed due to error
	if reader.Error() != nil {
		logrus.Errorf("snapshot process for topic %s failed: %v", topic, err)
		return
	}

	// Completed snapshot, publish magic eof
	b = []byte(creek.SnapEOF)

	mq.snapshotBus <- msg{
		subject:    topic,
		data:       b,
		identifier: fmt.Sprintf("%s-eof", header.LSN),
	}

}

func (mq *MQ) genSnapTopic(seed int64, timestamp time.Time, msg creek.SnapshotRequest) string {
	rng := rand.New(rand.NewSource(seed))

	timeStr := timestamp.Format("20060102150405.000000")

	randBuf := make([]byte, 2)

	rng.Read(randBuf)

	randStr := hex.EncodeToString(randBuf)

	escaped := strings.ReplaceAll(timeStr, ".", "_")

	return fmt.Sprintf("%s.%s.%s.%s_%s", mq.streamName(creek.SnapStream), msg.Namespace, msg.Table, escaped, randStr)
}
