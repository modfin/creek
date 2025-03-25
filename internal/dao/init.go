package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/modfin/creek/internal/metrics"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modfin/creek"
	"github.com/modfin/creek/internal/config"
	pgtypeavro "github.com/modfin/creek/pgtype-avro"
	"github.com/sirupsen/logrus"
)

const outputPlugin = "pgoutput"

type DB struct {
	ctx context.Context
	cfg config.Config

	typeMap *pgtype.Map

	config *pgxpool.Config
	pool   *pgxpool.Pool

	InitialLSN pglogrepl.LSN
}

func New(ctx context.Context, cfg config.Config) (*DB, error) {
	var db *DB

	poolCfg, err := pgxpool.ParseConfig(cfg.PgConfig.Uri)
	if err != nil {
		return db, fmt.Errorf("failed parse PostgreSQL config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return db, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	db = &DB{
		config:  poolCfg,
		ctx:     ctx,
		typeMap: pgtype.NewMap(),
		pool:    pool,
		cfg:     cfg,
	}

	return db, nil
}

func (db *DB) Connect(ctx context.Context) error {
	return db.ensureSchema(ctx)
}

func (db *DB) DatabaseName() string {
	return db.config.ConnConfig.Database
}

func (db *DB) ensureSchema(ctx context.Context) error {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`
		 CREATE SCHEMA IF NOT EXISTS _creek;
		 CREATE TABLE IF NOT EXISTS _creek.avro_schemas (
		    fingerprint TEXT,
		    schema TEXT,
		    source TEXT,
			at TIMESTAMPTZ DEFAULT now(),
			CONSTRAINT pk_avro_schema PRIMARY KEY (fingerprint)		                                                
		);
	`)

	if err != nil {
		return fmt.Errorf("failed to create _creek.avro_schemas table: %w", err)
	}

	_, err = tx.Exec(ctx, `
create or replace function _creek.remove_table(pub text, _tbl regclass)
returns void language plpgsql as $$

    begin
		EXECUTE 'ALTER PUBLICATION ' || quote_ident(pub) || ' DROP TABLE ' || _tbl;
		execute pg_notify('creek', 'REMOVE ' || _tbl);
    end;
    $$;

create or replace function _creek.add_table(pub text, _tbl regclass)
returns void language plpgsql as $$
    declare
        ns text;
    begin
        SELECT relnamespace::regnamespace::text
            FROM   pg_catalog.pg_class
            WHERE  oid = _tbl INTO ns;

        execute pg_notify('creek', 'ADD ' || ns || '.' || _tbl::name);
		EXECUTE 'ALTER PUBLICATION ' || quote_ident(pub) || ' ADD TABLE ' || _tbl;
    end;
    $$;
`)

	err = tx.Commit(ctx)
	return err
}

func (db *DB) ensurePublication(name string, tables []string) error {
	var exists bool
	var numSubscribed int
	err := db.pool.QueryRow(db.ctx, `
SELECT count(pub) > 0 AS exists, count(pub_rel) FROM pg_catalog.pg_publication pub
                LEFT JOIN pg_catalog.pg_publication_rel pub_rel ON pub.oid = pub_rel.prpubid
                WHERE pub.pubname = $1`, name).Scan(&exists, &numSubscribed)
	if err != nil {
		return fmt.Errorf("could not query for existing publications: %w", err)
	}
	if exists {
		metrics.SetSubscribedTables(numSubscribed)
		return nil
	}

	_, err = db.pool.Exec(db.ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %v WITH (publish_via_partition_root = true)",
		name,
		strings.Join(tables, ",")))
	if err != nil {
		return fmt.Errorf("create publication error: %w", err)
	}
	metrics.SetSubscribedTables(len(tables))
	logrus.Debugf("create publication %s", name)

	return nil
}

func (db *DB) connectSlot(ctx context.Context, name string, publicationName string) (conn *pgconn.PgConn, lsn pglogrepl.LSN, err error) {
	existing, err := db.pool.Query(ctx, "SELECT confirmed_flush_lsn FROM pg_get_replication_slots() WHERE slot_name = $1", name)
	if err != nil {
		err = fmt.Errorf("could not query for existing publications: %w", err)
		return
	}
	defer existing.Close()

	cfg := db.config.ConnConfig.Copy()
	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", publicationName)}
	cfg.RuntimeParams["replication"] = "database"

	pgxConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return
	}
	conn = pgxConn.PgConn() // lower level pgconn

	start := func(lsn pglogrepl.LSN) error {
		err = pglogrepl.StartReplication(ctx, conn, name, lsn, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})

		if err != nil {
			return fmt.Errorf("StartReplication failed: %w", err)
		}
		logrus.Infoln("logical replication started on slot:", publicationName)
		return nil
	}

	if existing.Next() {
		var flushLSN pglogrepl.LSN
		err = existing.Scan(&flushLSN)
		if err != nil {
			err = fmt.Errorf("failed to read confirmed_flush_lsn: %w", err)
			return
		}

		err = start(flushLSN)

		return conn, flushLSN, err
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		err = fmt.Errorf("IdentifySystem failed: %w", err)
		return
	}
	logrus.Debugln("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, name, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false, Mode: pglogrepl.LogicalReplication})
	if err != nil {
		err = fmt.Errorf("CreateReplicationSlot failed: %w", err)
		return
	}
	logrus.Debugln("created replication slot:", name)

	err = start(sysident.XLogPos)

	return conn, sysident.XLogPos, err
}

func (db *DB) StartReplication(tables []string, publicationName string, publicationSlot string) (*Replication, error) {
	err := db.ensurePublication(publicationName, tables)
	if err != nil {
		return nil, err
	}

	conn, lsn, err := db.connectSlot(db.ctx, publicationSlot, publicationName)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(db.ctx)
	rep := &Replication{
		parent:   db,
		ctx:      ctx,
		cancel:   cancel,
		doneChan: make(chan struct{}),

		conn:    conn,
		currLSN: lsn,
		xlogPos: lsn,
		prevLSN: lsn,

		messages:       make(chan creek.WAL, 1),
		schemaMessages: make(chan creek.SchemaMsg, 1),
		relations:      map[uint32]Relation{},
	}

	for _, table := range tables {
		err = db.initRelationSchema(table)
		if err != nil {
			logrus.Errorf("failed to initialize schema for table %s: %v", table, err)
		}
	}

	go db.startApi()
	go rep.start()

	return rep, nil
}

// initRelationSchema reads data about a relation (table) from the database, and saves a schema to _creek.schemas.
// when run on startup, it is possible for consumers to request the schema even if there have been no wal events yet.
func (db *DB) initRelationSchema(table string) error {
	tx, err := db.pool.Begin(db.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(db.ctx)

	// Split
	split := strings.SplitN(table, ".", 2)
	if len(split) != 2 {
		return errors.New("failed to parse table")
	}

	rel, err := getRelationInTx(tx, split[0], split[1])
	if err != nil {
		return err
	}
	err = tx.Commit(db.ctx)
	if err != nil {
		return err
	}

	schema, err := pgtypeavro.New(rel).RelationMessageToAvro()
	if err != nil {
		logrus.Error(err)
	}

	keySchema, err := pgtypeavro.New(rel).RelationMessageKeysToAvro()
	if err != nil {
		logrus.Error(err)
	}

	_, err = db.PersistSchemaFromRelation(Relation{Msg: rel, Schema: schema, KeySchema: keySchema})
	if err != nil {
		logrus.Errorf("failed to persist schema in database: %v", err)
	}

	return nil
}

func (db *DB) CanSnapshot(namespace string, table string) (bool, error) {
	var count int
	err := db.pool.QueryRow(db.ctx,
		"SELECT COUNT(*) FROM pg_catalog.pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
		db.cfg.PgConfig.PublicationName, namespace, table).Scan(&count)
	if err != nil || count == 0 {
		return false, err
	}

	return true, nil
}

func (db *DB) GetCurrLSN() (lsn pglogrepl.LSN, err error) {
	err = db.pool.QueryRow(db.ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn)
	return
}
