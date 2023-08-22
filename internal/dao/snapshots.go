package dao

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/modfin/creek"
	"github.com/modfin/creek/internal/metrics"
	pgtypeavro "github.com/modfin/creek/pgtype-avro"

	"github.com/hamba/avro/v2"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

type SnapshotReader struct {
	rows   chan map[string]interface{}
	err    error
	schema avro.Schema
}

func (s *SnapshotReader) Rows() <-chan map[string]interface{} {
	return s.rows
}

func (s *SnapshotReader) Error() error {
	return s.err
}

func (s *SnapshotReader) Schema() avro.Schema {
	return s.schema
}

func (db *DB) Snapshot(namespace string, table string) (header creek.SnapshotHeader, reader *SnapshotReader, err error) {
	tx, err := db.pool.BeginTx(context.Background(), pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		err = fmt.Errorf("could not begin transaction for snapshot, %w", err)
		return
	}

	rows, err := tx.Query(context.Background(), `
SELECT pg_current_wal_lsn(), txid_current(), now(), (
	SELECT c.reltuples::bigint AS estimate
	FROM   pg_class c
	JOIN   pg_namespace n ON n.oid = c.relnamespace
	WHERE  c.relname = $1
	AND    n.nspname = $2)
	`, table, namespace)

	if err != nil {
		err = fmt.Errorf("could not query lsn, txid and time, %s", err)
		return
	}
	if !rows.Next() {
		err = fmt.Errorf("could not retrive row for lsn, txid and time, %s", err)
		return
	}

	var lsn pglogrepl.LSN
	var txid uint32
	var at time.Time
	var estimate int
	err = rows.Scan(&lsn, &txid, &at, &estimate)
	if err != nil {
		err = fmt.Errorf("could not scan lsn, txid and time, %s", err)
		return
	}
	rows.Close()

	rel, err := getRelationInTx(tx, namespace, table)
	if err != nil {
		err = fmt.Errorf("could not get relation: %w", err)
		return
	}

	// Generate Avro schema
	schema, err := pgtypeavro.New(rel).RelationMessageToAvro()
	if err != nil {
		return
	}

	jsonSchema, err := json.Marshal(schema)
	if err != nil {
		return
	}
	avroSchema, err := avro.ParseBytes(jsonSchema)
	if err != nil {
		return
	}
	hash, err := avroSchema.FingerprintUsing(avro.CRC64Avro)
	if err != nil {
		return
	}
	fingerprint := base64.URLEncoding.EncodeToString(hash)

	header = creek.SnapshotHeader{
		Fingerprint: fingerprint,
		Schema:      string(jsonSchema),
		TxId:        txid,
		LSN:         lsn.String(),
		At:          at,
		ApproxRows:  estimate,
	}

	reader = &SnapshotReader{
		rows:   make(chan map[string]any, 1),
		schema: avroSchema,
	}

	go func() {

		defer func() {
			close(reader.rows)
			err = tx.Commit(context.Background())
			if err != nil {
				logrus.Errorf("failed to close tx: %v", err)
			}
		}()

		rows, err := tx.Query(db.ctx, fmt.Sprintf("SELECT * FROM %s.%s", namespace, table))
		if err != nil {
			err = fmt.Errorf("failed to select rows for snapshot for %s.%s: %v", namespace, table, err)
			reader.err = err
			return
		}
		descriptions := rows.FieldDescriptions()
		for rows.Next() {
			rowValues, err := rows.Values()

			if err != nil {
				logrus.Errorf("failed to select get row for snapshot for %s.%s: %v", namespace, table, err)
				return
			}

			rowData := make(map[string]any)
			for i, val := range rowValues {
				rowData[descriptions[i].Name] = val
			}

			values, err := pgtypeavro.MapToNativeTypes(rowData)
			if err != nil {
				logrus.Errorf("failed to map to native types when taking snapshot for %s.%s: %v", namespace, table, err)
				return
			}

			reader.rows <- values

			metrics.IncRead(creek.SnapStream,
				"read",
				fmt.Sprintf("%s.%s", namespace, table))
		}
	}()

	return header, reader, nil
}

func getRelationInTx(tx pgx.Tx, namespace string, table string) (rel *pglogrepl.RelationMessage, err error) {
	rows, err := tx.Query(context.Background(), `
	SELECT c.oid, c.relreplident 
	FROM pg_catalog.pg_class c
	INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
	WHERE n.nspname = $1 AND c.relname = $2`,
		namespace, table)
	if err != nil {
		return
	}
	defer rows.Close()

	var oid uint32
	var replIdent uint8

	for rows.Next() {
		err = rows.Scan(&oid, &replIdent)
		if err != nil {
			return
		}
	}

	rows, err = tx.Query(context.Background(), `
	SELECT attname, atttypid, atttypmod,
		CASE c.relreplident
			WHEN 'f' THEN true
			WHEN 'n' THEN false
			ELSE COALESCE(i.indisprimary, false)
		END AS is_key 
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_index i ON a.attrelid = i.indrelid
		AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON a.attrelid = c.oid
		WHERE attrelid = $1 AND attnum > 0
		AND attisdropped = false;`, oid)
	if err != nil {
		return
	}
	defer rows.Close()

	var attname string
	var atttypid uint32
	var atttypmod int32
	var primary bool
	var relationRows []*pglogrepl.RelationMessageColumn

	for rows.Next() {
		err = rows.Scan(&attname, &atttypid, &atttypmod, &primary)
		if err != nil {
			return
		}
		flags := uint8(0)
		if primary {
			flags |= 1
		}
		relationRows = append(relationRows, &pglogrepl.RelationMessageColumn{
			Flags:        flags,
			Name:         attname,
			DataType:     atttypid,
			TypeModifier: atttypmod,
		})
	}

	rel = &pglogrepl.RelationMessage{RelationID: oid,
		Namespace:       namespace,
		RelationName:    table,
		ReplicaIdentity: replIdent,
		ColumnNum:       uint16(len(relationRows)),
		Columns:         relationRows,
	}

	return rel, nil

}
