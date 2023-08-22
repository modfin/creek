package mq

import (
	"encoding/hex"
	"encoding/json"
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

func (mq *MQ) StartSnapshotAPI() error {

	err := mq.startSnapshotService()
	if err != nil {
		return err
	}
	return nil
}

func (mq *MQ) startSnapshotService() error {

	// Expect messages to be in json containing, example:
	// {"database": "db", "namespace": "public", "table": "data"}
	sub, err := mq.conn.Subscribe(fmt.Sprintf("%s._snapshot", mq.root), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}

		var received creek.SnapshotRequest
		err := json.Unmarshal(m.Data, &received)
		if err != nil {
			logrus.Errorf("[snapshot] failed to read json message for snapshot: %v", err)
			return
		}

		ok, err := mq.db.CanSnapshot(received.Namespace, received.Table)
		if err != nil {
			logrus.Errorf("[snapshot] failed to check if snapshot allowed: %v", err)
			return
		}
		if !ok {
			return
		}

		header, reader, err := mq.db.Snapshot(received.Namespace, received.Table)
		if err != nil {
			logrus.Errorf("[snapshot] failed to start snapshot for %s.%s", received.Namespace, received.Table)
			return
		}

		returnTopic := mq.genSnapTopic(int64(time.Now().Nanosecond()), time.Now(), received)

		// Respond with topic to client
		err = m.Respond([]byte(returnTopic))
		if err != nil {
			logrus.Errorf("[snapshot] failed to return snapshot topic for %s.%s", received.Namespace, received.Table)
			return
		}

		go mq.streamSnapshots(header, reader, returnTopic)
	})

	go func() {
		<-mq.ctx.Done()
		_ = sub.Drain()
	}()

	return err
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

	mq.publishBus <- msg{
		subject: topic,
		data:    b,
	}

	for row := range reader.Rows() {

		b, err := avro.Marshal(reader.Schema(), row)
		if err != nil {
			logrus.Errorf("failed to marshal snapshot data for topic %s, skipping message. err: %v", topic, err)
			continue
		}

		mq.publishBus <- msg{
			subject: topic,
			data:    b,
		}
	}

	// Snapshot failed due to error
	if reader.Error() != nil {
		logrus.Errorf("snapshot process for topic %s failed: %v", topic, err)
		return
	}

	// Completed snapshot, publish magic eof
	b = []byte(creek.SnapEOF)

	mq.publishBus <- msg{
		subject: topic,
		data:    b,
	}

}

func (mq *MQ) genSnapTopic(seed int64, timestamp time.Time, msg creek.SnapshotRequest) string {
	rng := rand.New(rand.NewSource(seed))

	timeStr := timestamp.Format("20060102150405.000000")

	randBuf := make([]byte, 2)

	rng.Read(randBuf)

	randStr := hex.EncodeToString(randBuf)

	escaped := strings.ReplaceAll(timeStr, ".", "_")

	return fmt.Sprintf("%s.%s.%s.%s.%s_%s", mq.ns, creek.SnapStream, msg.Namespace, msg.Table, escaped, randStr)
}
