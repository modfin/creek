package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/modfin/creek"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func (mq *MQ) ConsumeSchemaAPI() error {
	logrus.Infof("creating connection on %s for schema requests", mq.streamName(creek.SchemaStream))
	conn, err := mq.getConnection()
	if err != nil {
		return fmt.Errorf("could not get connection, err %v", err)
	}
	defer conn.Close()
	logrus.Infof("listening on %s for schema requests", mq.streamName(creek.SchemaStream))

	sub, err := conn.SubscribeSync(mq.streamName(creek.SchemaStream))

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
			err = mq.handleSchemaMessage(m)
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

func (mq *MQ) handleSchemaMessage(m *nats.Msg) error {
	if m.Reply == "" {
		return nil
	}
	fingerprint := string(m.Data)
	schema, err := mq.db.GetSchema(fingerprint)
	if err != nil {
		return fmt.Errorf("could not find schema for %s", fingerprint)
	}

	b, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("could not marshal schema for %s", fingerprint)
	}

	err = m.Respond(b)
	if err != nil {
		return fmt.Errorf("could not respond with schema for %s", fingerprint)
	}
	logrus.Debugf("sent schema for %s to client", fingerprint)
	return nil
}

type SchemaStream struct {
	doneChan chan struct{}
}

func (s *SchemaStream) Done() <-chan struct{} {
	return s.doneChan
}

func (mq *MQ) StartSchemaStream(stream <-chan creek.SchemaMsg) *SchemaStream {
	ss := SchemaStream{doneChan: make(chan struct{})}

	go func() {
		for schema := range stream {

			b, err := json.Marshal(schema)
			if err != nil {
				logrus.Errorf("could not find marshal schema for %s, dropping schema message", schema.Source)
				continue
			}

			mq.schemaBus <- msg{
				subject:    fmt.Sprintf("%s.%s", mq.streamName(creek.SchemaStream), schema.Source),
				data:       b,
				identifier: schema.Fingerprint,
			}

		}

		ss.doneChan <- struct{}{}
		logrus.Info("closed schema stream")

	}()

	return &ss
}
