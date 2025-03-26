package mq

import (
	"context"
	"encoding/binary"
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

func New(ctx context.Context, uri string, root string, db *dao.DB) (*MQ, error) {
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

	err = mq.connect()
	if err != nil {
		return nil, err
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

func (mq *MQ) connect() error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("could not get hostname, err %v", err)
	}
	hostname = strings.Split(hostname, ".")[0]
	opts := []nats.Option{
		nats.Name(hostname),
		nats.PingInterval(2 * time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			logrus.Info("reconnected to nats")
		}),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			logrus.Errorf("disconnected from nats: %v", err)
		}),
	}
	logrus.Infof("connecting to nats %s", mq.uri)
	mq.conn, err = nats.Connect(mq.uri, opts...)
	if err != nil {
		return fmt.Errorf("could not connect to nats, err %v", err)
	}

	mq.js, err = jetstream.New(mq.conn)
	if err != nil {
		return fmt.Errorf("could not create jetstream, err %v", err)
	}

	for _, t := range []creek.StreamType{creek.WalStream, creek.SnapStream, creek.SchemaStream} {
		err = mq.assignStream(t)
		if err != nil {
			return fmt.Errorf("could not assign stream, err %v", err)
		}
	}
	return nil
}

func (mq *MQ) startBus() {
	maxPayload := int(mq.conn.MaxPayload()) - 2 - 4 - 1

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

		sendLoop:
			for {
				_, err := mq.js.Publish(mq.ctx, m.subject, packet)
				if err != nil {
					logrus.Errorf("could not publish to nats on %s, err %v", m.subject, err)
					mq.reconnector()
				} else {
					break sendLoop
				}
			}
		}
	}
	logrus.Info("closed publish bus")
	mq.doneChan <- struct{}{}
}

func (mq *MQ) reconnector() {
	if !mq.conn.IsReconnecting() {
		err := mq.conn.ForceReconnect()
		// function code never return error
		if err != nil {
			logrus.Errorf("could not force reconnection")
		}
	}
	for {
		select {
		case <-mq.doneChan:
			return
		case <-mq.conn.StatusChanged(nats.CONNECTED):
			return
		case <-time.After(time.Second * 10):
			if mq.conn.IsConnected() {
				logrus.Info("reconnected to nats")
				return
			}
			logrus.Warn("failed to reconnect to nats, tries again")
			err := mq.conn.ForceReconnect()
			if err != nil {
				logrus.Errorf("could not force reconnection")
			}
		}
	}
}
