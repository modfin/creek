package mq

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/modfin/creek"

	"github.com/hamba/avro/v2"
	"github.com/sirupsen/logrus"
)

type WalStream struct {
	doneChan chan struct{}
}

func (w *WalStream) Done() <-chan struct{} {
	return w.doneChan
}

func (mq *MQ) StartWalStream(stream <-chan creek.WAL) *WalStream {

	ws := WalStream{doneChan: make(chan struct{})}

	go func() {
		for wal := range stream {

			wal.SentAt = time.Now()

			avroschema, err := mq.db.GetAvroSchema(wal.Fingerprint)
			if err != nil {
				logrus.Errorf("could not find schema for %s, dropping message from %s", wal.Fingerprint, wal.FullIdentifier())
				continue
			}

			content, err := avro.Marshal(avroschema, wal)
			if err != nil {
				logrus.Errorf("could not marshal wal to avroschema schema for %s, dropping message from %s, err: %v", wal.Fingerprint, wal.FullIdentifier(), err)
				continue
			}
			fingerprint, err := base64.URLEncoding.DecodeString(wal.Fingerprint)
			if err != nil {
				logrus.Errorf("could not decode fingerprint %s to binary, dropping message", wal.Fingerprint)
				continue
			}
			if len(fingerprint) != 8 {
				logrus.Errorf("fingerprint %s was not 8 bytes, dropping message", wal.Fingerprint)
				continue
			}
			header := append([]byte{0xc3, 0x01}, fingerprint...)

			data := header
			data = append(data, content...)

			// Avro Protocol
			//  [2 bytes] [8 bytes]   [data]
			//  ctrl      fingerprint

			logrus.Tracef("sending message to %s: %#v, before: %v, after: %v", fmt.Sprintf("%s.%s.%s", mq.ns, creek.WalStream, wal.LocalIdentifier()), wal, wal.Before, wal.After)

			mq.publishBus <- msg{
				subject: fmt.Sprintf("%s.%s.%s", mq.ns, creek.WalStream, wal.LocalIdentifier()),
				data:    data,
			}

		}

		ws.doneChan <- struct{}{}
		logrus.Info("closed wal stream")

	}()

	return &ws
}
