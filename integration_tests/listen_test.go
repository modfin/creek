package integration_tests

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/jackc/pglogrepl"
	"github.com/modfin/creek"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)

	db := GetDBConn()
	creekConn := GetCreekConn()

	_, err := db.Exec(TimeoutContext(time.Second), "INSERT INTO public.other VALUES (1, 'test');")
	assert.NoError(t, err)

	stream, err := creekConn.StreamWALFrom(TimeoutContext(time.Second*5), "public", "other", time.Now(), "0/0")
	assert.NoError(t, err)

	msg, err := stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

	assert.Greater(t, msg.Source.TxAt, time.Time{})
	assert.Greater(t, msg.SentAt, time.Time{})

	assert.Greater(t, msg.SentAt, msg.Source.TxAt)

	assert.Equal(t, DBname, msg.Source.DB)
	assert.Equal(t, "public", msg.Source.Schema)
	assert.Equal(t, "other", msg.Source.Table)
	assert.Equal(t, creek.OpInsert, msg.Op)

	assert.Nil(t, msg.Before)

	expected := make(map[string]any)
	expected["id"] = 1
	expected["data"] = "test"

	assert.Equal(t, &expected, msg.After)
	stream.Close()
	ts := msg.SentAt
	lsn := msg.Source.LSN

	_, err = db.Exec(TimeoutContext(time.Second), "INSERT INTO public.other VALUES (2, 'new stuff');")
	assert.NoError(t, err)

	stream, err = creekConn.StreamWALFrom(TimeoutContext(time.Second*5), "public", "other", time.Time{}, lsn)
	assert.NoError(t, err)

	msg, err = stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

	assert.Greater(t, msg.SentAt, ts)
	before, _ := pglogrepl.ParseLSN(lsn)
	after, _ := pglogrepl.ParseLSN(msg.Source.LSN)

	assert.Greater(t, after, before)

	expected = make(map[string]any)
	expected["id"] = 2
	expected["data"] = "new stuff"

	assert.Equal(t, &expected, msg.After)

	_, err = db.Exec(TimeoutContext(time.Second), `
		INSERT INTO public.other 
		VALUES (3, 'new stuff'),
		       (4, 'new stuff'),
		       (5, 'new stuff'),
		       (6, 'new stuff'),
		       (7, 'new stuff'),
		       (8, 'new stuff');
	`)
	assert.NoError(t, err)

	messages := countMessages(t.Context(), stream)
	assert.Equal(t, 6, messages)
}

func TestUpdate(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	now := time.Now()
	db := GetDBConn()
	creekConn := GetCreekConn()

	_, err := db.Exec(TimeoutContext(time.Second), "UPDATE public.other SET data='cool' WHERE id=1;")
	assert.NoError(t, err)
	_, err = db.Exec(TimeoutContext(time.Second), "UPDATE public.other SET id=100 WHERE id=1;")
	assert.NoError(t, err)

	stream, err := creekConn.StreamWALFrom(t.Context(), "public", "other", now, "0/0")

	assert.NoError(t, err)

	msg, err := stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

	assert.Equal(t, creek.OpUpdate, msg.Op)
	assert.Equal(t, &map[string]any{"id": 1}, msg.Before)
	assert.Equal(t, &map[string]any{"id": 1, "data": "cool"}, msg.After)

	msg, err = stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

	assert.Equal(t, creek.OpUpdatePk, msg.Op)
	assert.Equal(t, &map[string]any{"id": 1}, msg.Before)
	assert.Equal(t, &map[string]any{"id": 100, "data": "cool"}, msg.After)
}

func TestDelete(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	db := GetDBConn()
	creekConn := GetCreekConn()

	now := time.Now()
	_, err := db.Exec(TimeoutContext(time.Second), "DELETE FROM public.other WHERE id=100;")
	assert.NoError(t, err)

	stream, err := creekConn.StreamWALFrom(TimeoutContext(time.Second*5), "public", "other", now, "0/0")
	assert.NoError(t, err)

	msg, err := stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

	var nilMap *map[string]any = nil
	assert.Equal(t, creek.OpDelete, msg.Op)
	assert.Equal(t, &map[string]any{"id": 100}, msg.Before)
	assert.Equal(t, nilMap, msg.After)
}

func TestSnap(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	creekConn := GetCreekConn()

	reader, closeF, err := creekConn.Snapshot(t.Context(), "public", "types_data")
	assert.NoError(t, err)

	i := 0
	for range reader.Chan() {
		i++
	}

	assert.Equal(t, 1000, i)

	data, err := creekConn.ListSnapshots(t.Context(), "public", "types_data")
	assert.NoError(t, err)

	closeF()
	// Should return the same as above
	reader, closeF, err = creekConn.GetSnapshot(t.Context(), data[0].Name)
	assert.NoError(t, err)
	defer closeF()

	i = 0
	for range reader.Chan() {
		i++
	}

	assert.Equal(t, 1000, i)
}

func TestTypes(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	db := GetDBConn()
	creekConn := GetCreekConn()

	q := `
INSERT INTO public.types (bool, char, varchar, bpchar, date, float4, float8, int2, int4, int8, json, jsonb, text, time,
                   timestamp, timestamptz, uuid, numeric, boolArr, charArr, varcharArr, bpcharArr, dateArr, float4Arr,
                   float8Arr, int2Arr, int4Arr, int8Arr, jsonArr, jsonbArr, textArr, timeArr, timestampArr,
                   timestamptzArr, uuidArr, numericArr)
VALUES (true, 'a', 'hi', 'hello', '2023-01-23', 0.23, 12.32, 123, 231, 123123, '{
  "hello": "world"
}', '{
  "hello": "world"
}', 'text', '12:30', now(), now(), gen_random_uuid(), '231.112',
        '{true, false}', '{a}', '{hi}', '{hello}', '{2023-01-23}', '{0.23}', '{12.32}', '{123}', '{231}', '{123123}', '{}',
        '{}', '{text}', '{12:30, 13:20}', '{now()}', '{now(), now()}', '{46145d05-8bc7-403b-8098-1baf99e97b56}', '{231.112}')
        `

	_, err := db.Exec(TimeoutContext(time.Second), q)
	assert.NoError(t, err)

	stream, err := creekConn.StreamWAL(TimeoutContext(time.Second*5), "public", "types")
	assert.NoError(t, err)

	_, err = stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

}

func TestSchema(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	creekConn := GetCreekConn()

	expectedJSON := `
{
    "name": "publish_message",
    "type": "record",
    "fields": [
        {
            "name": "fingerprint",
            "type": "string"
        },
        {
            "name": "source",
            "type": {
                "name": "source",
                "type": "record",
                "fields": [
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "tx_at",
                        "type": {
                            "type": "long",
                            "logicalType": "timestamp-micros"
                        }
                    },
                    {
                        "name": "db",
                        "type": "string"
                    },
                    {
                        "name": "schema",
                        "type": "string"
                    },
                    {
                        "name": "table",
                        "type": "string"
                    },
                    {
                        "name": "tx_id",
                        "type": "long"
                    },
                    {
                        "name": "lsn",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "op",
            "type": {
                "name": "op",
                "type": "enum",
                "symbols": [
                    "c",
                    "u",
                    "u_pk",
                    "d",
                    "t",
                    "r"
                ]
            }
        },
        {
            "name": "sent_at",
            "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        },
        {
            "name": "before",
            "type": [
                "null",
                {
                    "name": "before.types",
                    "type": "record",
                    "fields": [
                        {
                            "name": "uuid",
                            "type": {
                                "type": "string",
                                "logicalType": "uuid"
                            },
                            "pgKey": true,
                            "pgType": "uuid"
                        }
                    ]
                }
            ]
        },
        {
            "name": "after",
            "type": [
                "null",
                {
                    "name": "after.types",
                    "type": "record",
                    "fields": [
                        {
                            "name": "bool",
                            "type": [
                                "null",
                                "boolean"
                            ],
                            "pgKey": false,
                            "pgType": "bool"
                        },
                        {
                            "name": "char",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "bpchar"
                        },
                        {
                            "name": "varchar",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "varchar"
                        },
                        {
                            "name": "bpchar",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "bpchar"
                        },
                        {
                            "name": "date",
                            "type": [
                                "null",
                                {
                                    "type": "int",
                                    "logicalType": "date"
                                },
                                {
                                    "name": "after.infinity_modifier",
                                    "type": "enum",
                                    "symbols": [
                                        "infinity",
                                        "negative_infinity_ca5991f51367e3e4"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "date"
                        },
                        {
                            "name": "float4",
                            "type": [
                                "null",
                                "float"
                            ],
                            "pgKey": false,
                            "pgType": "float4"
                        },
                        {
                            "name": "float8",
                            "type": [
                                "null",
                                "double"
                            ],
                            "pgKey": false,
                            "pgType": "float8"
                        },
                        {
                            "name": "int2",
                            "type": [
                                "null",
                                "int"
                            ],
                            "pgKey": false,
                            "pgType": "int2"
                        },
                        {
                            "name": "int4",
                            "type": [
                                "null",
                                "int"
                            ],
                            "pgKey": false,
                            "pgType": "int4"
                        },
                        {
                            "name": "int8",
                            "type": [
                                "null",
                                "long"
                            ],
                            "pgKey": false,
                            "pgType": "int8"
                        },
                        {
                            "name": "json",
                            "type": [
                                "null",
                                "bytes"
                            ],
                            "pgKey": false,
                            "pgType": "json"
                        },
                        {
                            "name": "jsonb",
                            "type": [
                                "null",
                                "bytes"
                            ],
                            "pgKey": false,
                            "pgType": "jsonb"
                        },
                        {
                            "name": "text",
                            "type": [
                                "null",
                                "string"
                            ],
                            "pgKey": false,
                            "pgType": "text"
                        },
                        {
                            "name": "time",
                            "type": [
                                "null",
                                {
                                    "type": "long",
                                    "logicalType": "time-micros"
                                },
                                "after.infinity_modifier"
                            ],
                            "pgKey": false,
                            "pgType": "time"
                        },
                        {
                            "name": "timestamp",
                            "type": [
                                "null",
                                {
                                    "type": "long",
                                    "logicalType": "timestamp-micros"
                                },
                                "after.infinity_modifier"
                            ],
                            "pgKey": false,
                            "pgType": "timestamp"
                        },
                        {
                            "name": "timestamptz",
                            "type": [
                                "null",
                                {
                                    "type": "long",
                                    "logicalType": "timestamp-micros"
                                },
                               	"after.infinity_modifier"
                            ],
                            "pgKey": false,
                            "pgType": "timestamptz"
                        },
                        {
                            "name": "uuid",
                            "type": {
                                "type": "string",
                                "logicalType": "uuid"
                            },
                            "pgKey": true,
                            "pgType": "uuid"
                        },
                        {
                            "name": "numeric",
                            "type": [
                                "null",
                                {
                                    "type": "bytes",
                                    "logicalType": "decimal",
                                    "precision": 10,
									"scale": 5
								}
                            ],
                            "pgKey": false,
                            "pgType": "numeric"
                        },
                        {
                            "name": "boolarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "boolean"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_bool"
                        },
                        {
                            "name": "chararr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_bpchar"
                        },
                        {
                            "name": "varchararr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_varchar"
                        },
                        {
                            "name": "bpchararr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_bpchar"
                        },
                        {
                            "name": "datearr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "int",
                                            "logicalType": "date"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_date"
                        },
                        {
                            "name": "float4arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "float"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_float4"
                        },
                        {
                            "name": "float8arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "double"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_float8"
                        },
                        {
                            "name": "int2arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "int"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_int2"
                        },
                        {
                            "name": "int4arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "int"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_int4"
                        },
                        {
                            "name": "int8arr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "long"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_int8"
                        },
                        {
                            "name": "jsonarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "bytes"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_json"
                        },
                        {
                            "name": "jsonbarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "bytes"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_jsonb"
                        },
                        {
                            "name": "textarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_text"
                        },
                        {
                            "name": "timearr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "long",
                                            "logicalType": "time-micros"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_time"
                        },
                        {
                            "name": "timestamparr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "long",
                                            "logicalType": "timestamp-micros"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_timestamp"
                        },
                        {
                            "name": "timestamptzarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "long",
                                            "logicalType": "timestamp-micros"
                                        },
                                        "after.infinity_modifier"
                                    ]
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_timestamptz"
                        },
                        {
                            "name": "uuidarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                        "logicalType": "uuid"
                                    }
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_uuid"
                        },
                        {
                            "name": "numericarr",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "bytes",
                                        "logicalType": "decimal",
                                        "precision": 10,
										"scale": 5
									}
                                }
                            ],
                            "pgKey": false,
                            "pgType": "_numeric"
                        }
                    ]
                }
            ]
        }
    ]
}`

	schema, err := creekConn.GetLastSchema(TimeoutContext(time.Second), "public", "types")
	assert.NoError(t, err)

	assert.JSONEq(t, expectedJSON, schema.Schema)

	avroSchema, err := avro.Parse(schema.Schema)
	assert.NoError(t, err)

	fingerprint, err := avroSchema.FingerprintUsing(avro.CRC64Avro)
	assert.NoError(t, err)
	encoded := base64.URLEncoding.EncodeToString(fingerprint)

	assert.Equal(t, encoded, schema.Fingerprint)

	schema, err = creekConn.GetSchema(TimeoutContext(time.Second), encoded)
	assert.NoError(t, err)
	assert.JSONEq(t, expectedJSON, schema.Schema)
}

func TestPartitions(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	db := GetDBConn()
	creekConn := GetCreekConn()

	_, err := db.Exec(TimeoutContext(time.Second), "INSERT INTO public.prices VALUES (1, 43.01, '2022-09-13'), (2, 16.98, '2023-09-13');")
	assert.NoError(t, err)

	stream, err := creekConn.StreamWAL(t.Context(), "public", "prices")
	assert.NoError(t, err)

	msg, err := stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)

	assert.Equal(t, DBname, msg.Source.DB)
	assert.Equal(t, "public", msg.Source.Schema)
	assert.Equal(t, "prices", msg.Source.Table)

	msg, err = stream.Next(TimeoutContext(time.Second))
	assert.NoError(t, err)
	assert.Equal(t, "public", msg.Source.Schema)
	assert.Equal(t, "prices", msg.Source.Table)
}

func TestRestart(t *testing.T) {
	teardownTest := setupTest(t)
	defer teardownTest(t)
	db := GetDBConn()
	creekConn := GetCreekConn()

	msgCounter.Reset()

	// Disable network proxy to simulate DB disconnect
	err := DisableProxi()
	assert.NoError(t, err)

	time.Sleep(time.Second * 3)

	// Reenable network proxy to simulate DB reconnect
	err = EnableProxi()
	assert.NoError(t, err)

	time.Sleep(time.Second * 3)

	now := time.Now()
	_, err = db.Exec(TimeoutContext(time.Second), "INSERT INTO public.other VALUES (999, 'reconnect');")
	assert.NoError(t, err)

	stream, err := creekConn.StreamWALFrom(t.Context(), "public", "other", now, "0/0")
	assert.NoError(t, err)

	msg, err := stream.Next(TimeoutContext(time.Second * 2))
	assert.NoError(t, err)

	logMsgs := msgCounter.Msgs()
	assert.Greater(t, logMsgs, 0)
	assert.Less(t, logMsgs, 100)

	assert.Equal(t, creek.OpInsert, msg.Op)
	assert.Equal(t, &map[string]any{"id": 999, "data": "reconnect"}, msg.After)
}
