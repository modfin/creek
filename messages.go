package creek

import (
	"time"

	pgtypeavro "github.com/modfin/creek/pgtype-avro"
)

type Op string

// StreamType types of stream
type StreamType string

const WalStream StreamType = "wal"
const SnapStream StreamType = "snap"
const SchemaStream StreamType = "schema"

const (
	OpInsert   Op = "c"
	OpUpdate   Op = "u"
	OpUpdatePk Op = "u_pk" // Update operation that changed the primary key
	OpDelete   Op = "d"
	OpTruncate Op = "t"
)

// MessageSource source information about a WAL message
type MessageSource struct {
	Name    string    `json:"name" avro:"name"`
	TxAt    time.Time `json:"tx_at" avro:"tx_at"`
	DB      string    `json:"db" avro:"db"`
	Schema  string    `json:"schema" avro:"schema"`
	Table   string    `json:"table" avro:"table"`
	TxId    uint32    `json:"tx_id" avro:"tx_id"`
	LastLSN string    `json:"last_lsn" avro:"last_lsn"`
	LSN     string    `json:"lsn" avro:"lsn"`
}

type WAL struct {
	Fingerprint string                  `json:"fingerprint" avro:"fingerprint"`
	Source      MessageSource           `json:"source" avro:"source"`
	Op          Op                      `json:"op" avro:"op"`
	SentAt      time.Time               `json:"sent_at" avro:"sent_at"`
	Before      *map[string]interface{} `json:"before" avro:"before"`
	After       *map[string]interface{} `json:"after" avro:"after"`
}

// FullIdentifier returns the full identifier of the WAL message, ie db.namespace.table
func (w WAL) FullIdentifier() string {
	return w.Source.DB + "." + w.Source.Schema + "." + w.Source.Table
}

// LocalIdentifier returns the local identifier of the WAL message, ie namespace.table
func (w WAL) LocalIdentifier() string {
	return w.Source.Schema + "." + w.Source.Table
}

// AvroSchema returns a full WAL message Avro schema based on the before and after schemas
func AvroSchema(before, after *pgtypeavro.Record) pgtypeavro.Schema {
	before.Namespace = "before"
	after.Namespace = "after"
	return &pgtypeavro.Record{
		Type: pgtypeavro.TypeRecord,
		Name: "publish_message",
		Fields: []*pgtypeavro.RecordField{
			{Name: "fingerprint", Type: pgtypeavro.TypeStr},
			{Name: "source", Type: &pgtypeavro.Record{
				Type: pgtypeavro.TypeRecord,
				Name: "source",
				Fields: []*pgtypeavro.RecordField{
					{Name: "name", Type: pgtypeavro.TypeStr},
					{Name: "tx_at", Type: &pgtypeavro.DerivedType{Type: pgtypeavro.TypeLong, LogicalType: pgtypeavro.LogicalTypeTimestampMicros}},
					{Name: "db", Type: pgtypeavro.TypeStr},
					{Name: "schema", Type: pgtypeavro.TypeStr},
					{Name: "table", Type: pgtypeavro.TypeStr},
					{Name: "tx_id", Type: pgtypeavro.TypeLong},
					{Name: "last_lsn", Type: pgtypeavro.TypeStr},
					{Name: "lsn", Type: pgtypeavro.TypeStr},
				},
			}},
			{Name: "op", Type: &pgtypeavro.Enum{
				Name:    "op",
				Type:    pgtypeavro.TypeEnum,
				Symbols: []string{"c", "u", "u_pk", "d", "t", "r"},
			}},
			{Name: "sent_at", Type: &pgtypeavro.DerivedType{Type: pgtypeavro.TypeLong, LogicalType: pgtypeavro.LogicalTypeTimestampMicros}},
			{Name: "before", Type: &pgtypeavro.Union{pgtypeavro.TypeNull, before}},
			{Name: "after", Type: &pgtypeavro.Union{pgtypeavro.TypeNull, after}},
		},
	}
}

// SnapshotRequest RPC message when requesting a snapshot
type SnapshotRequest struct {
	Database  string `json:"database"`
	Namespace string `json:"namespace"`
	Table     string `json:"table"`
}

// SchemaMsg messages emitted to the schema stream and returned using the schema API
type SchemaMsg struct {
	Fingerprint string    `json:"fingerprint"`
	Schema      string    `json:"schema"`
	Source      string    `json:"source"`
	CreatedAt   time.Time `json:"created_at"`
}

// SnapshotHeader the first message on a snapshot channel
type SnapshotHeader struct {
	Topic       string    `json:"topic"`
	Fingerprint string    `json:"fingerprint"`
	Schema      string    `json:"schema"`
	TxId        uint32    `json:"tx_id"`
	LSN         string    `json:"lsn"`
	At          time.Time `json:"at"`
	ApproxRows  int       `json:"approx_rows"`
}

// SnapEOF message that is sent at end of snapshot
const SnapEOF = "EOF"
