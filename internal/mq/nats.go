package mq

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/modfin/henry/slicez"

	"github.com/modfin/creek"

	"github.com/modfin/creek/internal/config"
	"github.com/modfin/creek/internal/dao"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

type MQ struct {
	ctx       context.Context
	uri       string
	root      string
	snapWg    sync.WaitGroup
	snapsDone chan struct{}
	closeOnce sync.Once
	doneChan  chan struct{}

	cfg  config.NatsConfig
	db   *dao.DB
	conn *nats.Conn

	js jetstream.JetStream

	streams    map[string]jetstream.Stream
	publishBus chan msg
}

type msg struct {
	subject string
	data    []byte
}

//func (mq *MQ) Done() <-chan interface{} {
//	//return chanz.EveryDone(mq.ctx.Done())
//	return make(<-chan interface{})
//}

//func (mq *MQ) walStreamName() string {
//	return fmt.Sprintf("%s.wal", mq.ns)
//}
//func (mq *MQ) snapStreamName() string {
//	return fmt.Sprintf("%s.snap", mq.ns)
//}
//func (mq *MQ) schemaStreamName() string {
//	return fmt.Sprintf("%s.schema", mq.ns)
//}

func (mq *MQ) streamName(_type creek.StreamType) string {
	return fmt.Sprintf("%s_%s_%s", mq.root, _type, mq.db.DatabaseName())
}
func (mq *MQ) assignStream(_type creek.StreamType) error {
	streamName := mq.streamName(_type)

	stream, err := mq.js.CreateOrUpdateStream(mq.ctx, jetstream.StreamConfig{
		Name:        streamName,
		Replicas:    mq.cfg.Replicas,
		Description: "Creek stream relating to " + streamName,
		Subjects:    []string{fmt.Sprintf("%s.>", streamName)},
		MaxAge:      mq.cfg.Retention.MaxAge,
		MaxBytes:    mq.cfg.Retention.MaxBytes,
		MaxMsgs:     mq.cfg.Retention.MaxMsgs,
		Retention:   mq.cfg.Retention.Policy,
	})
	if err != nil {
		return err
	}
	mq.streams[streamName] = stream
	return nil
}

func New(ctx context.Context, uri string, root string, maxPending int, db *dao.DB) (*MQ, error) {
	var err error
	mq := &MQ{
		ctx:        ctx,
		uri:        uri,
		root:       root,
		db:         db,
		streams:    map[string]jetstream.Stream{},
		publishBus: make(chan msg, 1),
		doneChan:   make(chan struct{}),
		snapsDone:  make(chan struct{}),
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	hostname = strings.Split(hostname, ".")[0]

	opts := []nats.Option{
		nats.Name(hostname),
		nats.PingInterval(2 * time.Second),
		nats.MaxReconnects(-1),
	}

	mq.uri = uri
	mq.conn, err = nats.Connect(uri, opts...)
	if err != nil {
		return nil, err
	}

	mq.js, err = jetstream.New(mq.conn, jetstream.WithPublishAsyncMaxPending(maxPending))
	if err != nil {
		return nil, err
	}

	for _, t := range []creek.StreamType{creek.WalStream, creek.SnapStream, creek.SchemaStream} {
		err = mq.assignStream(t)
		if err != nil {
			return nil, err
		}
	}

	go mq.startBus()

	go func() {
		<-ctx.Done()
		mq.snapWg.Wait()
		logrus.Info("snaps done")

		mq.snapsDone <- struct{}{}
	}()

	return mq, nil
}

func (mq *MQ) SnapsDone() <-chan struct{} {
	return mq.snapsDone
}

func (mq *MQ) Close() {
	mq.closeOnce.Do(func() {
		close(mq.publishBus)
	})
}

func (mq *MQ) Done() <-chan struct{} {
	return mq.doneChan
}

func (mq *MQ) startBus() {
	maxPayload := int(mq.conn.MaxPayload()) - 2 - 4 - 1

	publish := func(subject string, data []byte) {
		for {
			_, err := mq.js.PublishAsync(subject, data)
			if errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
				logrus.Tracef("nats async buffer is full, waiting to compleat")
				<-mq.js.PublishAsyncComplete()
				logrus.Tracef("nats async buffer is empty, continuing")
				continue
			}
			if err != nil {
				logrus.Errorf("could not publish to nats on %s, err %v", subject, err)
			}
			break
		}
	}

	for m := range mq.publishBus {
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(m.data)))
		packets := uint16(1 + (len(m.data))/maxPayload)

		for i := uint16(0); i < packets; i++ {
			seq := make([]byte, 2)
			binary.BigEndian.PutUint16(seq, i)

			packet := seq
			if i == 0 {
				packet = append(packet, length...)
			}
			ii := int(i)
			// TODO integration_tests off by 1 stuff....
			packet = append(packet, m.data[ii*maxPayload:slicez.Min((ii+1)*maxPayload, len(m.data))]...)

			publish(m.subject, packet)
		}
	}
	logrus.Info("closed publish bus")
	mq.doneChan <- struct{}{}
}
