package mq

import (
	"encoding/json"
	"fmt"

	"github.com/modfin/creek"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

func (mq *MQ) StartSchemaAPI() error {

	err := mq.startSchemaLookupService()
	if err != nil {
		return err
	}
	return nil
}

func (mq *MQ) startSchemaLookupService() error {

	logrus.Infof("listening on %s._schema for schema requests", mq.root)

	sub, err := mq.conn.Subscribe(fmt.Sprintf("%s._schema", mq.root), func(m *nats.Msg) {
		if m.Reply == "" {
			return
		}
		fingerprint := string(m.Data)
		schema, err := mq.db.GetSchema(fingerprint)
		if err != nil {
			logrus.Debugf("[schema lookup] could not find schema for %s, err %v", fingerprint, err)
			return
		}

		b, err := json.Marshal(schema)
		if err != nil {
			logrus.Errorf("[schema lookup] could not marshal schema for %s", fingerprint)
			return
		}

		err = m.Respond(b)
		if err != nil {
			logrus.Errorf("[schema lookup] could not respond with schema for %s", fingerprint)
			return
		}
		logrus.Debugf("sent schema for %s to client", fingerprint)

	})

	go func() {
		<-mq.ctx.Done()
		_ = sub.Drain()
	}()

	return err
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

			mq.publishBus <- msg{
				subject: fmt.Sprintf("%s.%s.%s", mq.ns, creek.SchemaStream, schema.Source),
				data:    b,
			}

		}

		ss.doneChan <- struct{}{}
		logrus.Info("closed schema stream")

	}()

	return &ss
}
