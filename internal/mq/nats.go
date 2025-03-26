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

	cfg config.NatsConfig
	db  *dao.DB

	walBusDoneChan      chan struct{}
	schemaBusDoneChan   chan struct{}
	snapshotBusDoneChan chan struct{}

	walBus      chan msg
	schemaBus   chan msg
	snapshotBus chan msg

	busDone     chan struct{}
	busDoneOnce sync.Once
}

type msg struct {
	subject    string
	identifier string
	data       []byte
}

func (mq *MQ) streamName(_type creek.StreamType) string {
	return fmt.Sprintf("%s_%s_%s", mq.root, _type, mq.db.DatabaseName())
}

func New(ctx context.Context, uri string, root string, db *dao.DB) (*MQ, error) {
	mq := &MQ{
		ctx:                 ctx,
		uri:                 uri,
		root:                root,
		db:                  db,
		walBus:              make(chan msg, 1),
		schemaBus:           make(chan msg, 1),
		snapshotBus:         make(chan msg, 1),
		walBusDoneChan:      make(chan struct{}),
		schemaBusDoneChan:   make(chan struct{}),
		snapshotBusDoneChan: make(chan struct{}),
		snapsDone:           make(chan struct{}),

		busDone:     make(chan struct{}),
		busDoneOnce: sync.Once{},
	}

	go mq.startBus(creek.WalStream)
	go mq.startBus(creek.SchemaStream)
	go mq.startBus(creek.SnapStream)
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
		close(mq.walBus)
		close(mq.schemaBus)
		close(mq.snapshotBus)
	})
}

func (mq *MQ) Done() <-chan struct{} {
	go func() {
		mq.busDoneOnce.Do(func() {
			<-mq.walBusDoneChan
			<-mq.schemaBusDoneChan
			<-mq.snapshotBusDoneChan
			mq.busDone <- struct{}{}
		})
	}()
	return mq.busDone
}

func (mq *MQ) getConnection() (*nats.Conn, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("could not get hostname, err %v", err)
	}
	hostname = strings.Split(hostname, ".")[0]
	opts := []nats.Option{
		nats.Name(hostname),
		nats.PingInterval(2 * time.Second),
	}
	logrus.Infof("connecting to nats %s", mq.uri)
	conn, err := nats.Connect(mq.uri, opts...)
	if err != nil {
		return nil, fmt.Errorf("could not connect to nats, err %v", err)
	}
	return conn, nil
}
func (mq *MQ) getJetstream() (*jetstream.JetStream, error) {
	conn, err := mq.getConnection()
	if err != nil {
		return nil, fmt.Errorf("could not get connection, err %v", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("could not create jetstream, err %v", err)
	}
	return &js, nil
}

// TODO: extend startBus to send commited lsn to replication to sync db replication with published messages on nats
func (mq *MQ) startBus(_type creek.StreamType) {
	var bus <-chan msg
	var doneChan chan struct{}
	switch _type {
	case creek.WalStream:
		bus = mq.walBus
		doneChan = mq.walBusDoneChan
	case creek.SchemaStream:
		bus = mq.schemaBus
		doneChan = mq.schemaBusDoneChan
	case creek.SnapStream:
		bus = mq.snapshotBus
		doneChan = mq.snapshotBusDoneChan
	}

	maxPayload := 0
maxPayloadLoop:
	for {
		select {
		case <-mq.ctx.Done():
			return
		default:
			conn, err := mq.getConnection()
			if err != nil {
				logrus.Errorf("could not get connection to determine max payload size, err %v", err)
				time.Sleep(time.Second * 2)
				continue
			}
			maxPayload = int(conn.MaxPayload()) - 2 - 4 - 1
			conn.Close()
			break maxPayloadLoop
		}
	}

	packetsChan := make(chan packetMessage, 1)

	var unhandledMessage *packetMessage
	var err error
	go func() {
		for {
			select {
			case <-mq.ctx.Done():
				return
			default:
				unhandledMessage, err = mq.consumePackets(mq.streamName(_type), packetsChan, unhandledMessage)
				if err != nil {
					logrus.Errorf("could not consume packets, err %v", err)
					time.Sleep(time.Second * 2)
					continue
				}
			}
		}
	}()

	for m := range bus {
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
			packetsChan <- packetMessage{subject: m.subject, data: packet, id: fmt.Sprintf("%s-seq-%d", m.identifier, i)}
		}

		// push message on chan to replication here with m.identifier
	}

	logrus.Infof("closed %s bus", _type)
	doneChan <- struct{}{}
}

type packetMessage struct {
	subject string
	id      string
	data    []byte
}

func (mq *MQ) consumePackets(streamName string, packetsChan <-chan packetMessage, unhandledMessage *packetMessage) (*packetMessage, error) {
	js, err := mq.getJetstream()
	if err != nil {
		return nil, fmt.Errorf("could not create jetstream, err %v", err)
	}
	if js == nil {
		return nil, fmt.Errorf("nil jetstream object, err")
	}
	defer (*js).Conn().Close()

	logrus.Infof("upserting nats stream %s", streamName)
	_, err = (*js).CreateOrUpdateStream(mq.ctx, jetstream.StreamConfig{
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
		return nil, fmt.Errorf("could not create nats stream, err %v", err)
	}

	if unhandledMessage != nil {
		_, err = (*js).Publish(mq.ctx, unhandledMessage.subject, unhandledMessage.data, jetstream.WithMsgID(unhandledMessage.id))
		if err != nil && errors.Is(err, context.Canceled) {
			logrus.Errorf("context canceled with a unhandled package, err %v", err)
			return nil, nil
		}
		if err != nil {
			return unhandledMessage, err
		}
	}

	for {
		select {
		case <-mq.ctx.Done():
			return nil, nil
		case packet := <-packetsChan:
			_, err := (*js).Publish(mq.ctx, packet.subject, packet.data, jetstream.WithMsgID(packet.id))
			if err != nil {
				return &packet, err
			}

		}
	}
}
