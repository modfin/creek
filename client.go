package creek

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/modfin/henry/chanz"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/hamba/avro/v2"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type lsn uint64

func init() {
	avro.Register(string(avro.String)+"."+string(avro.UUID), "")
	avro.Register(string(avro.Array), []interface{}{})
	avro.Register("before.infinity_modifier", "")
	avro.Register("after.infinity_modifier", "")
}

type Logger interface {
	Info(format string, args ...interface{})
	Debug(format string, args ...interface{})
	Error(format string, args ...interface{})
}
type simpleLogger struct{}

func (d simpleLogger) Info(format string, args ...interface{}) {
	log.Print("info: ", fmt.Sprintf(format, args...))
}

func (d simpleLogger) Debug(string, ...interface{}) {}

func (d simpleLogger) Error(format string, args ...interface{}) {
	log.Printf("error: %s", fmt.Sprintf(format, args...))
}

type Client struct {
	uri    string
	rootNs string
	db     string

	log Logger

	natsOpts      []nats.Option
	jetstreamOpts []jetstream.JetStreamOpt
}

// SnapRow a snapshot data row
type SnapRow map[string]any

type Error struct {
	Message string
}

var ErrNoSchemaFound = Error{Message: "no schema found"}

var schemaCache, _ = lru.New[string, avro.Schema](256)

type Conn struct {
	nc *nats.Conn
	js jetstream.JetStream

	streams map[StreamType]jetstream.Stream

	parent *Client
}

// NewClient Creates a new creek client
func NewClient(natsUri string, rootNamespace string, db string) *Client {
	logger := simpleLogger{}
	return &Client{uri: natsUri, rootNs: rootNamespace, db: db, log: logger}
}

func (c *Client) With(opts ...func(c *Client)) *Client {
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func LoggerOpt(log Logger) func(c *Client) {
	return func(c *Client) {
		c.log = log
	}
}

func NatsOptions(opts ...nats.Option) func(c *Client) {
	return func(c *Client) {
		c.natsOpts = opts
	}
}

func JetstreamOptions(opts ...jetstream.JetStreamOpt) func(c *Client) {
	return func(c *Client) {
		c.jetstreamOpts = opts
	}
}

func (c *Client) WithLogger(log Logger) {
	c.With(LoggerOpt(log))
}

func (c *Client) WithNatsOptions(opts ...nats.Option) {
	c.With(NatsOptions(opts...))
}

func (c *Client) WithJetstreamOptions(opts ...jetstream.JetStreamOpt) {
	c.With(JetstreamOptions(opts...))
}

func (c *Client) GetStreamName(streamType StreamType) string {
	return fmt.Sprintf("%s_%s_%s", c.rootNs, streamType, c.db)
}

// Connect Connects the Client to nats
func (c *Client) Connect(ctx context.Context) (*Conn, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	hostname = strings.Split(hostname, ".")[0]

	// Default options that can be overridden by the client
	opts := []nats.Option{
		nats.Name(hostname),
		nats.PingInterval(2 * time.Second),
	}
	c.natsOpts = append(opts, c.natsOpts...)

	nc, err := nats.Connect(c.uri, c.natsOpts...)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc, c.jetstreamOpts...)
	if err != nil {
		return nil, err
	}

	streams := make(map[StreamType]jetstream.Stream)
	for _, streamType := range []StreamType{WalStream, SnapStream, SchemaStream} {
		stream, err := js.Stream(ctx, c.GetStreamName(streamType))
		if err != nil {
			return nil, err
		}
		streams[streamType] = stream
	}
	conn := &Conn{js: js, nc: nc, streams: streams, parent: c}

	return conn, nil
}

func (c *Conn) Close() {
	if c != nil {
		c.nc.Close()
	}
}

// GetSchema requests a schema from Creek. If no schema is found, it will hang. Please use with
// a context with an appropriate timeout
func (c *Conn) GetSchema(ctx context.Context, fingerprint string) (SchemaMsg, error) {
	var schema SchemaMsg

	resp, err := c.nc.RequestWithContext(ctx, c.parent.GetStreamName(SchemaStream), []byte(fingerprint))
	if err != nil {
		return schema, err
	}

	err = json.Unmarshal(resp.Data, &schema)
	if err != nil {
		return schema, err
	}

	return schema, nil
}

// GetLastSchema returns the latest schema if it exists.
func (c *Conn) GetLastSchema(ctx context.Context, tableSchema string, table string) (schema SchemaMsg, err error) {
	msg, err := c.streams[SchemaStream].GetLastMsgForSubject(ctx, fmt.Sprintf("%s.%s.%s", c.parent.GetStreamName(SchemaStream), tableSchema, table))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return schema, ErrNoSchemaFound
	}
	if err != nil {
		return
	}

	// TODO: Can this become larger than 1 message? If so, we need to handle it
	packet := msg.Data
	if len(packet) < 2 || packet[0] != 0 || packet[1] != 0 {
		return schema, errors.New("last found schema message was larger than one nats message. This is currently unsupported")
	}

	err = json.Unmarshal(msg.Data[6:], &schema)
	return
}

type WALStream struct {
	msgs  <-chan WAL
	close chan struct{}
}

// StreamWALFrom opens a consumer for the database and table topic. The table topic should be in the form `<STEAM_NAME>.<DATABASE_SCHEMA>.<DATABASE_TABLE>`.
// Starts streaming from the first message with the timestamp AND log sequence number (lsn) that is greater than the one provided.
func (c *Conn) StreamWALFrom(ctx context.Context, tableSchema string, table string, timestamp time.Time, lsn string) (stream *WALStream, err error) {
	topic := fmt.Sprintf("%s.%s.%s", c.parent.GetStreamName(WalStream), tableSchema, table)
	c.parent.log.Debug(fmt.Sprintf("starting streaming WAL messages on topic %s", topic))

	parsedLSN, err := parseLSN(lsn)
	if err != nil {
		return stream, fmt.Errorf("failed to parse LSN: %w", err)
	}

	consumer, err := c.streams[WalStream].OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{topic},
		DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
		OptStartTime:   &timestamp,
	})
	if err != nil {
		return stream, err
	}

	iter, err := consumer.Messages()
	if err != nil {
		return stream, err
	}

	closeChan := make(chan struct{})

	msgChan := unmarshalStream(iter, closeChan, c.parent.log, func(data []byte) (wal WAL, done bool, err error) {
		prefix := data[:2]
		if prefix[0] != 0xc3 || prefix[1] != 0x01 {
			err = fmt.Errorf("received non-avro message: %s. Ignoring", hex.Dump(data))
			return
		}
		fingerprint := base64.URLEncoding.EncodeToString(data[2:10])
		schema, err := c.getAvroSchema(ctx, fingerprint)
		if err != nil {
			err = fmt.Errorf("failed to get schema for message with fingerprint %s. %v", fingerprint, err)
			return
		}

		var walMsg WAL
		err = avro.Unmarshal(schema, data[10:], &walMsg)
		if err != nil {
			err = fmt.Errorf("failed deserialize avro massage with fingerprint %s: %v", fingerprint, err)
			return
		}

		return walMsg, done, nil
	})

	msgChan = chanz.DropWhile(msgChan, func(msg WAL) bool {
		msgLSN, _ := parseLSN(msg.Source.LSN)
		return msgLSN <= parsedLSN
	}, chanz.OpBuffer(1))

	return &WALStream{msgs: msgChan, close: closeChan}, nil
}

// StreamWAL opens a consumer for the database and table topic. The table topic should be in the form `<STEAM_NAME>.<DATABASE_SCHEMA>.<DATABASE_TABLE>`.
func (c *Conn) StreamWAL(ctx context.Context, tableSchema string, table string) (stream *WALStream, err error) {
	topic := fmt.Sprintf("%s.%s.%s", c.parent.GetStreamName(WalStream), tableSchema, table)
	c.parent.log.Info(fmt.Sprintf("starting streaming WAL messages on topic %s", topic))

	consumer, err := c.streams[WalStream].OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{topic},
	})

	if err != nil {
		return stream, err
	}

	iter, err := consumer.Messages()
	if err != nil {
		return stream, err
	}

	closeChan := make(chan struct{})

	msgChan := unmarshalStream(iter, closeChan, c.parent.log, func(data []byte) (wal WAL, done bool, err error) {
		prefix := data[:2]
		if prefix[0] != 0xc3 || prefix[1] != 0x01 {
			err = fmt.Errorf("received non-avro message: %s. Ignoring", hex.Dump(data))
			return
		}
		fingerprint := base64.URLEncoding.EncodeToString(data[2:10])
		schema, err := c.getAvroSchema(ctx, fingerprint)
		if err != nil {
			err = fmt.Errorf("failed to get schema for message with fingerprint %s. %v", fingerprint, err)
			return
		}

		var walMsg WAL
		err = avro.Unmarshal(schema, data[10:], &walMsg)
		if err != nil {
			err = fmt.Errorf("failed deserialize avro massage with fingerprint %s: %v", fingerprint, err)
			return
		}

		return walMsg, done, nil
	})

	return &WALStream{msgs: msgChan, close: closeChan}, nil
}

// Close closes the WALStream. Can only be called once
func (ws *WALStream) Close() {
	ws.close <- struct{}{}
}

// Next Returns the next message in the WAL stream. Blocks until a message is received.
func (ws *WALStream) Next(ctx context.Context) (msg WAL, err error) {
	for {
		select {
		case <-ctx.Done():
			return WAL{}, context.Canceled
		case msg, ok := <-ws.msgs:
			if !ok {
				return msg, errors.New("no more messages to receive")
			}

			return msg, nil
		}

	}
}

type SnapshotReader struct {
	schema avro.Schema
	header SnapshotHeader
	rows   <-chan SnapRow
}

// Snapshot request a new snapshot from creek. Returns a blocking channel containing snapshot data rows.
func (c *Conn) Snapshot(ctx context.Context, tableSchema string, table string) (*SnapshotReader, error) {
	msg := SnapshotRequest{
		Database:  c.parent.db,
		Namespace: tableSchema, // rootNs is used as namespace for streams and is called root as we don't use schema for the pg schema (instead we use namespace)
		Table:     table,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, time.Second)
	resp, err := c.nc.RequestWithContext(reqCtx, c.parent.GetStreamName(SnapStream), b)
	cancel()
	if err != nil {
		return nil, err
	}

	topic := string(resp.Data)

	consumer, err := c.streams[SnapStream].OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{topic},
	})
	if err != nil {
		return nil, err
	}

	iter, err := consumer.Messages()
	if err != nil {
		return nil, err
	}

	// Read header
	header, err := readMessage(iter)
	if err != nil {
		return nil, err
	}
	var snapHeader SnapshotHeader
	err = json.Unmarshal(header, &snapHeader)
	if err != nil {
		return nil, err
	}

	snapHeader.Topic = topic

	avroSchema, err := avro.Parse(snapHeader.Schema)
	if err != nil {
		return nil, err
	}

	// Read the rest of the messages
	streamMsgs := unmarshalStream(iter, make(<-chan struct{}), c.parent.log, func(data []byte) (SnapRow, bool, error) {
		if isEof(data) {
			return nil, true, nil
		}

		var rowData map[string]any
		err = avro.Unmarshal(avroSchema, data, &rowData)
		if err != nil {
			err = fmt.Errorf("failed deserialize snapshot avro massage: %v, message: %s", err, hex.Dump(data))
			return nil, false, err
		}

		return rowData, false, nil
	})

	return &SnapshotReader{
		schema: avroSchema,
		header: snapHeader,
		rows:   streamMsgs,
	}, nil
}

func (c *Conn) GetSnapshot(ctx context.Context, topic string) (*SnapshotReader, error) {
	consumer, err := c.streams[SnapStream].OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{topic},
	})
	if err != nil {
		return nil, err
	}

	iter, err := consumer.Messages()
	if err != nil {
		return nil, err
	}

	i, err := consumer.Info(ctx)
	if err != nil {
		return nil, err
	}
	// No messages here
	if i.NumPending == 0 {
		return nil, errors.New("snapshot topic has no messages")
	}

	// Read header
	header, err := readMessage(iter)
	if err != nil {
		return nil, err
	}
	var snapHeader SnapshotHeader
	err = json.Unmarshal(header, &snapHeader)
	if err != nil {
		return nil, err
	}

	snapHeader.Topic = topic

	avroSchema, err := avro.Parse(snapHeader.Schema)
	if err != nil {
		return nil, err
	}

	// Read the rest of the messages
	streamMsgs := unmarshalStream(iter, make(<-chan struct{}), c.parent.log, func(data []byte) (SnapRow, bool, error) {
		if isEof(data) {
			return nil, true, nil
		}

		var rowData map[string]any
		err = avro.Unmarshal(avroSchema, data, &rowData)
		if err != nil {
			err = fmt.Errorf("failed deserialize snapshot avro massage: %v, message: %s", err, hex.Dump(data))
			return nil, false, err
		}

		return rowData, false, nil
	})

	return &SnapshotReader{
		schema: avroSchema,
		header: snapHeader,
		rows:   streamMsgs,
	}, nil
}

type SnapMetadata struct {
	Name     string
	At       time.Time
	Messages uint64
}

// ListSnapshots returns a sorted list (in ascending order by creation date) of existing snapshots for a particular table
func (c *Conn) ListSnapshots(ctx context.Context, tableSchema string, table string) ([]SnapMetadata, error) {
	info, err := c.streams[SnapStream].Info(ctx, jetstream.WithSubjectFilter(fmt.Sprintf("%s.%s.%s.*", c.parent.GetStreamName(SnapStream), tableSchema, table)))
	if err != nil {
		return nil, err
	}

	keys := slicez.Sort(mapz.Keys(info.State.Subjects))

	var snaps []SnapMetadata
	for _, key := range keys {

		suffix := key[strings.LastIndex(key, ".")+1 : len(key)-5]
		parsed, err := time.Parse("20060102150405.000000", strings.ReplaceAll(suffix, "_", "."))
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %w", err)
		}

		snaps = append(snaps, SnapMetadata{
			Name:     key,
			At:       parsed,
			Messages: info.State.Subjects[key] - 2, // First and last are not rows
		})
	}

	return snaps, nil
}

func isEof(data []byte) bool {
	return len(data) == 3 && data[0] == SnapEOF[0] && data[1] == SnapEOF[1] && data[2] == SnapEOF[2]
}

func (err Error) Error() string {
	return err.Message
}

// Next returns the next snapshot row. Blocks until next message is received.
func (sr *SnapshotReader) Next() (map[string]any, error) {
	for {
		msg, ok := <-sr.rows
		if !ok {
			return msg, io.EOF
		}

		return msg, nil
	}
}

// Chan returns a channel over snapshot row data. Blocks until next message is received.
func (sr *SnapshotReader) Chan() <-chan SnapRow {
	return sr.rows
}

// Keys returns the primary keys for this table
func (sr *SnapshotReader) Keys() []string {

	var keys []string
	switch val := sr.schema.(type) {
	case *avro.RecordSchema:
		for _, f := range val.Fields() {
			key := f.Prop("pgKey")
			if key == false {
				continue
			}

			keys = append(keys, f.Name())
		}
	}
	return keys
}

func (sr *SnapshotReader) Header() SnapshotHeader {
	return sr.header
}

func drainMessage(iter jetstream.MessagesContext) error {
	for {
		msg, err := iter.Next()
		if err != nil {
			return err
		}
		packet := msg.Data()
		if len(packet) > 1 && packet[0] == 0 && packet[1] == 0 {
			_ = msg.Nak()
			return nil
		}
	}
}

func readMessage(iter jetstream.MessagesContext) ([]byte, error) {
	var seq uint16
	var msgBytes uint32
	var data []byte

	msg, err := iter.Next()
	if err != nil {
		return nil, err
	}
	_ = msg.Ack()
	packet := msg.Data()
	if len(packet) < 2 || packet[0] != 0 || packet[1] != 0 {
		_ = drainMessage(iter)
		return nil, errors.New("could not read message, sequence ")
	}

	first := true

	for {
		if !first {
			first = false
			msg, err = iter.Next()
			if err != nil {
				return nil, err
			}
			msg.Ack()
		}

		packet := msg.Data()
		seq = binary.BigEndian.Uint16(packet[0:2])
		if seq == 0 {
			// First message contains msg length
			data = []byte{}
			msgBytes = binary.BigEndian.Uint32(packet[2:6])
		}

		data = append(data, packet[6:]...)

		if uint32(len(data)) < msgBytes {
			continue
		}
		return data, nil
	}

}

func unmarshalStream[T any](iter jetstream.MessagesContext, closeChan <-chan struct{}, log Logger, handler func([]byte) (T, bool, error)) <-chan T {

	msgChan := make(chan T, 1)
	readMsgChan := make(chan struct {
		data []byte
		err  error
	})

	readMsg := func() {
		data, err := readMessage(iter)
		readMsgChan <- struct {
			data []byte
			err  error
		}{data: data, err: err}
	}

	go func() {
		for {
			go readMsg()

			var data []byte
			var err error
			select {
			case <-closeChan:
				close(msgChan)
				iter.Stop()
				return

			case msg := <-readMsgChan:
				data = msg.data
				err = msg.err
			}

			if err != nil {
				log.Error(fmt.Sprintf("failed to read message: %v", err))
				continue
			}
			t, done, err := handler(data)

			if done {
				close(msgChan)
				iter.Stop()
				return
			}

			if err != nil {
				log.Error(fmt.Sprintf("failed to unmarshal message: %v", err))
			} else {
				msgChan <- t
			}

		}
	}()

	return msgChan
}

func (c *Conn) getAvroSchema(ctx context.Context, fingerprint string) (avro.Schema, error) {

	avroSchema, ok := schemaCache.Get(fingerprint)
	if ok {
		return avroSchema, nil
	}

	c.parent.log.Info(fmt.Sprintf("Requesting schema for fingerprint %s on topic %s", fingerprint, c.parent.GetStreamName(SchemaStream)))

	resp, err := c.nc.RequestWithContext(ctx, c.parent.GetStreamName(SchemaStream), []byte(fingerprint))
	if err != nil {
		return nil, err
	}

	c.parent.log.Info(fmt.Sprintf("Recieved schema for fingerptint %s", fingerprint))

	var schemaMsg SchemaMsg

	err = json.Unmarshal(resp.Data, &schemaMsg)
	if err != nil {
		return nil, err
	}

	avroSchema, err = avro.Parse(schemaMsg.Schema)
	if err != nil {
		return nil, err
	}

	schemaCache.Add(fingerprint, avroSchema)

	return avroSchema, nil
}

// ParseLSN parses the given XXX/XXX text format LSN used by PostgreSQL.
// From https://github.com/jackc/pglogrepl/blob/d0818e1fbef75e7a3e2f6887e022959904dae6a2/pglogrepl.go#L95
func parseLSN(s string) (lsn, error) {
	var upperHalf uint64
	var lowerHalf uint64
	var nparsed int
	nparsed, err := fmt.Sscanf(s, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN: %w", err)
	}

	if nparsed != 2 {
		return 0, fmt.Errorf("failed to parsed LSN: %s", s)
	}

	return lsn((upperHalf << 32) + lowerHalf), nil
}
