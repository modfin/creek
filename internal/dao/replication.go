package dao

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/modfin/creek/internal/metrics"

	"github.com/modfin/creek"
	pgtypeavro "github.com/modfin/creek/pgtype-avro"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

type Replication struct {
	parent   *DB
	ctx      context.Context
	cancel   func()
	closing  sync.Once
	doneChan chan struct{}

	intx bool

	conn     *pgconn.PgConn
	currLSN  pglogrepl.LSN
	prevLSN  pglogrepl.LSN
	txid     uint32
	commitAt time.Time

	messages       chan creek.WAL
	schemaMessages chan creek.SchemaMsg

	relations map[uint32]Relation
}

func (r *Replication) Next() (creek.WAL, error) {
	m, ok := <-r.Stream()
	if !ok {
		return m, errors.New("replication is closed")
	}
	return m, nil
}
func (r *Replication) Stream() <-chan creek.WAL {
	return r.messages
}

func (r *Replication) NextSchema() (creek.SchemaMsg, error) {
	m, ok := <-r.SchemaStream()
	if !ok {
		return m, errors.New("replication is closed")
	}
	return m, nil
}
func (r *Replication) SchemaStream() <-chan creek.SchemaMsg {
	return r.schemaMessages
}

func (r *Replication) Close() {
	r.cancel()
}
func (r *Replication) close() {
	r.closing.Do(func() {
		close(r.messages)
		close(r.schemaMessages)
		r.sendStatusUpdate()
		r.conn.Close(context.Background())
		r.doneChan <- struct{}{}
		logrus.Info("closed replication")

	})
}

func (r *Replication) Done() <-chan struct{} {
	return r.doneChan
}

func (r *Replication) sendStatusUpdate() {
	err := pglogrepl.SendStandbyStatusUpdate(r.ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: r.currLSN})
	if err != nil {
		logrus.Errorln("SendStandbyStatusUpdate failed:", err)
	}
	logrus.Traceln("Sent Standby status message")
}

func (r *Replication) start() {

	timeout := time.Now().Add(time.Second * 10)

	for {
		select {
		case <-r.ctx.Done():
			if !r.intx {
				r.close()
				return
			}
		default:
		}

		ctx, cancel := context.WithDeadline(r.ctx, timeout)

		var rawMsg pgproto3.BackendMessage
		var err error
		// Receive message from logical replication
		rawMsg, err = r.conn.ReceiveMessage(ctx)
		cancel()
		if errors.Is(err, context.Canceled) {
			// In transaction, we want to continue reading all data in transaction
			if r.intx {
				rawMsg, err = r.conn.ReceiveMessage(context.Background())
			} else {
				continue
			}
		}

		if pgconn.Timeout(err) {
			r.sendStatusUpdate()
			timeout = time.Now().Add(time.Minute)
			continue
		}

		if err != nil {
			logrus.Errorf("recieveMessage error: %+v", err)

			if err.Error() == "conn closed" {
				connectCtx, cancel := context.WithTimeout(r.ctx, time.Second*5)
				err = r.tryReconnect(connectCtx)
				if err != nil {
					logrus.Errorf("failed to reconnect to database: %v, retrying", err)
				}
				cancel()
			}
			continue
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			logrus.Errorf("received Postgres WAL error: %+v", errMsg)
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			logrus.Errorf("received unexpected message: %+v", rawMsg)
			continue
		}

		var logicalMsg pglogrepl.Message
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			// Keepalive message
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				logrus.Errorf("ParsePrimaryKeepaliveMessage failed: %v", err)
				continue
			}
			logrus.Traceln("Primary Keepalive Message =>",
				"ServerWALEnd:", pkm.ServerWALEnd,
				"ServerTime:", pkm.ServerTime,
				"ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				timeout = time.Time{}
				continue
			}

		case pglogrepl.XLogDataByteID:
			// XLog data
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				logrus.Errorf("ParseXLogData failed: %v", err)
				continue
			}
			r.currLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			logrus.Tracef("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n%s\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime, hex.Dump(xld.WALData))

			// Parse into logical replication message
			logicalMsg, err = pglogrepl.Parse(xld.WALData)
			if err != nil {
				logrus.Errorf("parse logical replication message: %v", err)
				continue
			}
			logrus.Debugf("Received a logical replication message: [LSN: %s, %s]\n", r.currLSN, logicalMsg.Type())

			switch logicalMsg := logicalMsg.(type) {
			case *pglogrepl.BeginMessage:
				r.intx = true
				r.handleBeginMessage(logicalMsg)

			case *pglogrepl.RelationMessage:
				err = r.handleRelationMessage(logicalMsg)
				if err != nil {
					logrus.Errorf("failed to handle relation message: %v", err)
				}
			case *pglogrepl.TypeMessage:
				r.handleTypeMessage(logicalMsg)

			case *pglogrepl.CommitMessage:
				r.intx = false
				// I think this does nothing useful,
				// since we have all necessary information from the begin message.

			case *pglogrepl.InsertMessage:
				err := r.handleInsertMessage(logicalMsg)
				if err != nil {
					logrus.Errorf("failed to handle insert message: %v", err)
				}

			case *pglogrepl.UpdateMessage:
				err := r.handleUpdateMessage(logicalMsg)
				if err != nil {
					logrus.Errorf("failed to handle update message: %v", err)
				}

			case *pglogrepl.DeleteMessage:
				err := r.handleDeleteMessage(logicalMsg)
				if err != nil {
					logrus.Errorf("failed to handle delete message: %v", err)
				}

			case *pglogrepl.TruncateMessage:
				// Do we really need truncate?
				err := r.handleTruncateMessage(logicalMsg)
				if err != nil {
					logrus.Errorf("failed to handle truncate message: %v", err)
				}
			}

		}

	}

}

func (r *Replication) handleBeginMessage(msg *pglogrepl.BeginMessage) {
	// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions.
	// You won't get any events from rolled back transactions.
	r.prevLSN = r.currLSN
	r.currLSN = msg.FinalLSN
	r.commitAt = msg.CommitTime
	r.txid = msg.Xid
}

func (r *Replication) handleRelationMessage(msg *pglogrepl.RelationMessage) error {
	// todo move to state with custom types if necessary

	schema, err := pgtypeavro.New(msg).RelationMessageToAvro()
	if err != nil {
		logrus.Error(err)
	}

	keySchema, err := pgtypeavro.New(msg).RelationMessageKeysToAvro()
	if err != nil {
		logrus.Error(err)
	}

	rel := Relation{Msg: msg, Schema: schema, KeySchema: keySchema}

	rel.fingerprint, err = r.parent.PersistSchemaFromRelation(rel)
	if err != nil {
		logrus.Errorf("failed to persist schema in database: %v", err)
	}

	r.relations[msg.RelationID] = rel

	avroSchema, err := r.parent.GetAvroSchema(rel.fingerprint)
	if err != nil {
		logrus.Errorf("failed to find schema: %v", err)
	}

	rawSchema, err := json.Marshal(avroSchema)
	if err != nil {
		logrus.Errorf("failed to marshal schema to json: %v", err)
	}

	// Send a message that we have a new schema
	r.schemaMessages <- creek.SchemaMsg{
		Fingerprint: rel.fingerprint,
		Source:      fmt.Sprintf("%s.%s", msg.Namespace, msg.RelationName),
		CreatedAt:   time.Now(),
		Schema:      string(rawSchema),
	}

	metrics.IncRead(creek.SchemaStream,
		"relation",
		fmt.Sprintf("%s.%s", rel.Msg.Namespace, rel.Msg.RelationName))

	return err
}

func (r *Replication) handleTypeMessage(msg *pglogrepl.TypeMessage) {
	//ds.AvroTypeMap.RegisterType(&pgtypeavro.SQLType{Name: msg.Name, OID: msg.DataType})
}

func (r *Replication) handleInsertMessage(msg *pglogrepl.InsertMessage) error {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	//// Remove from snapshot window
	//if ds.SnapshotManager.shouldRemoveData(msg.RelationID) {
	//	keyVals := ds.getKeyValues(msg.Tuple.Columns, rel)
	//	ds.SnapshotManager.removeKeys(keyVals)
	//}
	//
	message := r.baseMessage(rel)

	message.Op = creek.OpInsert
	values, err := pgtypeavro.MapToNativeTypes(r.getValues(msg.Tuple.Columns, rel))
	if err != nil {
		return fmt.Errorf("failed to map to native types: %w", err)
	}

	message.After = &values

	r.messages <- message

	metrics.IncRead(creek.WalStream,
		"insert",
		fmt.Sprintf("%s.%s", rel.Msg.Namespace, rel.Msg.RelationName))

	return nil
}

func (r *Replication) handleUpdateMessage(msg *pglogrepl.UpdateMessage) error {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// // Received a snapshot event
	// if rel.Msg.Namespace == "snapshot" && rel.Msg.RelationName == "window" {
	// 	received := ds.getValues(msg.NewTuple.Columns, rel)["value"]
	// 	ds.SnapshotManager.handleWindowEvent(received)
	// 	err := ds.SnapshotManager.MaybePublishWindow(ds, p)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }

	// // Remove from snapshot window
	// if ds.SnapshotManager.shouldRemoveData(msg.RelationID) {
	// 	keyVals := ds.getKeyValues(msg.NewTuple.Columns, rel)
	// 	ds.SnapshotManager.removeKeys(keyVals)
	// }

	message := r.baseMessage(rel)

	var before map[string]interface{}
	if msg.OldTupleType == 'K' || msg.OldTupleType == 'O' {
		message.Op = creek.OpUpdatePk
		before = r.getValues(msg.OldTuple.Columns, rel)
	} else {
		message.Op = creek.OpUpdate
		before = r.getKeyValues(msg.NewTuple.Columns, rel)
	}

	beforeValues, err := pgtypeavro.MapToNativeTypes(before)
	if err != nil {
		return fmt.Errorf("failed to map to native types: %w", err)
	}

	afterValues, err := pgtypeavro.MapToNativeTypes(r.getValues(msg.NewTuple.Columns, rel))
	if err != nil {
		return fmt.Errorf("failed to map to native types: %w", err)
	}

	message.After = &afterValues
	message.Before = &beforeValues

	r.messages <- message

	metrics.IncRead(creek.WalStream,
		"update",
		fmt.Sprintf("%s.%s", rel.Msg.Namespace, rel.Msg.RelationName))

	return nil

}

func (r *Replication) handleDeleteMessage(msg *pglogrepl.DeleteMessage) error {
	rel, ok := r.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// // Remove from snapshot window
	// if ds.SnapshotManager.shouldRemoveData(msg.RelationID) {
	// 	keyVals := ds.getKeyValues(msg.OldTuple.Columns, rel)
	// 	ds.SnapshotManager.removeKeys(keyVals)
	// }

	message := r.baseMessage(rel)
	message.Op = creek.OpDelete

	var values *map[string]interface{}

	if msg.OldTupleType == 'K' || msg.OldTupleType == 'O' {
		before := r.getValues(msg.OldTuple.Columns, rel)
		native, err := pgtypeavro.MapToNativeTypes(before)
		values = &native
		if err != nil {
			return fmt.Errorf("failed to map to native types: %w", err)
		}
	}

	message.Before = values

	r.messages <- message

	metrics.IncRead(creek.WalStream,
		"delete",
		fmt.Sprintf("%s.%s", rel.Msg.Namespace, rel.Msg.RelationName))

	return nil
}

func (r *Replication) handleTruncateMessage(msg *pglogrepl.TruncateMessage) error {

	for _, relationID := range msg.RelationIDs {

		rel, ok := r.relations[relationID]
		if !ok {
			return fmt.Errorf("unknown relation ID: %d", relationID)
		}

		// // Remove from snapshot window
		// if ds.SnapshotManager.shouldRemoveData(relationID) {
		// 	ds.SnapshotManager.clearAll()
		// }

		message := r.baseMessage(rel)
		message.Op = creek.OpTruncate

		r.messages <- message

		metrics.IncRead(creek.WalStream,
			"truncate",
			fmt.Sprintf("%s.%s", rel.Msg.Namespace, rel.Msg.RelationName))
	}

	return nil
}

func (r *Replication) getValues(cols []*pglogrepl.TupleDataColumn, rel Relation) map[string]interface{} {
	values := map[string]interface{}{}
	for idx, col := range cols {
		colName := rel.Msg.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': //text
			val, err := decodeTextColumnData(r.parent.typeMap, col.Data, rel.Msg.Columns[idx].DataType)
			if err != nil {
				logrus.Fatalln("error decoding column data:", err)
			}
			values[colName] = val
		}
	}

	return values
}

func (r *Replication) getKeyValues(cols []*pglogrepl.TupleDataColumn, rel Relation) map[string]interface{} {
	values := map[string]interface{}{}
	for idx, col := range cols {
		colRel := rel.Msg.Columns[idx]
		// Skip non-key values
		if colRel.Flags != 1 {
			continue
		}
		val, err := decodeTextColumnData(r.parent.typeMap, col.Data, rel.Msg.Columns[idx].DataType)
		if err != nil {
			logrus.Fatalln("error decoding column data:", err)
		}
		values[colRel.Name] = val
	}

	return values
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {

	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (r *Replication) baseMessage(rel Relation) creek.WAL {
	msg := creek.WAL{
		Fingerprint: rel.fingerprint,
		Source: creek.MessageSource{
			Name:    "dummy",
			TxAt:    r.commitAt,
			DB:      r.parent.config.ConnConfig.Database,
			Schema:  rel.Msg.Namespace,
			Table:   rel.Msg.RelationName,
			TxId:    r.txid,
			LastLSN: r.prevLSN.String(),
			LSN:     r.currLSN.String(),
		},
	}
	return msg
}

func (r *Replication) tryReconnect(ctx context.Context) error {
	conn, _, err := r.parent.connectSlot(ctx, r.parent.cfg.PgPublicationSlot, r.parent.cfg.PgPublicationName)
	if err != nil {
		return err
	}
	r.conn = conn
	return nil
}
