package dao

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/modfin/creek"

	"github.com/hamba/avro/v2"
	lru "github.com/hashicorp/golang-lru/v2"
)

var avroSchemaCache, _ = lru.New[string, avro.Schema](256) // Magic number...

func (db *DB) GetSchema(fingerprint string) (creek.SchemaMsg, error) {
	var schema creek.SchemaMsg

	var schemaStr string
	var source string
	var at time.Time

	err := db.pool.QueryRow(context.Background(), `
	SELECT schema, source, at FROM _creek.avro_schemas WHERE  fingerprint = $1 
	    `, fingerprint).Scan(&schemaStr, &source, &at)
	if err != nil {
		return schema, err
	}

	schema = creek.SchemaMsg{
		Fingerprint: fingerprint,
		Schema:      schemaStr,
		Source:      source,
		CreatedAt:   at,
	}

	return schema, nil
}

func (db *DB) GetAvroSchema(fingerprint string) (avro.Schema, error) {

	avroschema, ok := avroSchemaCache.Get(fingerprint)
	if ok {
		return avroschema, nil
	}

	var str string

	err := db.pool.QueryRow(context.Background(), `
	SELECT schema FROM _creek.avro_schemas WHERE  fingerprint = $1 
	`, fingerprint).Scan(&str)
	if err != nil {
		return nil, err
	}

	avroschema, err = avro.Parse(str)
	if err != nil {
		return nil, err
	}

	avroSchemaCache.Add(fingerprint, avroschema)

	return avroschema, nil
}

func (db *DB) PersistSchemaFromRelation(relation Relation) (string, error) {

	fingerprint, avroSchema, err := relation2Schemas(relation)
	if err != nil {
		return "", err
	}

	avroSchemaCache.Add(fingerprint, avroSchema)

	rawSchema, err := json.Marshal(avroSchema)
	if err != nil {
		return "", err
	}

	source := fmt.Sprintf("%s.%s", relation.Msg.Namespace, relation.Msg.RelationName)

	rows, err := db.pool.Query(context.Background(), `
	INSERT INTO _creek.avro_schemas (fingerprint, schema, source)
	    VALUES 	($1, $2, $3) ON CONFLICT DO NOTHING
	`, fingerprint, string(rawSchema), source)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return fingerprint, err
}

func relation2Schemas(relation Relation) (fingerprint string, avroSchema avro.Schema, err error) {
	raw := creek.AvroSchema(relation.KeySchema, relation.Schema)
	jsonSchema, err := json.Marshal(raw)
	if err != nil {
		return
	}
	avroSchema, err = avro.ParseBytes(jsonSchema)
	if err != nil {
		return
	}
	hash, err := avroSchema.FingerprintUsing(avro.CRC64Avro)
	if err != nil {
		return
	}
	fingerprint = base64.URLEncoding.EncodeToString(hash)

	return fingerprint, avroSchema, nil
}
