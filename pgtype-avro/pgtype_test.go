package pgtypeavro

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestRelationMessageToAvro(t *testing.T) {

	type testCase struct {
		name     string
		input    *pglogrepl.RelationMessage
		expected string
	}

	tests := []testCase{
		{
			name: "simple",
			input: &pglogrepl.RelationMessage{
				Namespace:       "public",
				RelationName:    "integration_tests",
				ReplicaIdentity: 'd',
				Columns: []*pglogrepl.RelationMessageColumn{
					{Flags: 1, Name: "id", DataType: pgtype.Int8OID},
					{Flags: 0, Name: "name", DataType: pgtype.TextOID},
				}},
			expected: `
			{
				"type": "record",
				"name": "integration_tests",
				"fields": [
					{
						"name": "id",
						"type": "long",
						"pgType": "int8",
						"pgKey": true
					},
					{
						"name": "name",
						"type": [
							"null",
							"string"
						],
						"pgType": "text",
						"pgKey": false
					}
				]
			}`,
		}, {
			name: "logical types",
			input: &pglogrepl.RelationMessage{
				Namespace:       "public",
				RelationName:    "integration_tests",
				ReplicaIdentity: 'd',
				Columns: []*pglogrepl.RelationMessageColumn{
					{Flags: 1, Name: "id", DataType: pgtype.Int8OID},
					{Flags: 0, Name: "uuid", DataType: pgtype.UUIDOID},
					{Flags: 0, Name: "timestamp", DataType: pgtype.TimestampOID},
					{Flags: 0, Name: "date", DataType: pgtype.DateOID},
					{Flags: 0, Name: "time", DataType: pgtype.TimeOID},
					{Flags: 0, Name: "timestamptz", DataType: pgtype.TimestamptzOID},
					// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c#L897
					{Flags: 0, Name: "numeric", DataType: pgtype.NumericOID, TypeModifier: ((23 << 16) | (13 & 0x7ff)) + 4},
				}},
			expected: `
			{
				"type": "record",
				"name": "integration_tests",
				"fields": [
					{
						"name": "id",
						"type": "long",
						"pgType": "int8",
						"pgKey": true
					},
					{
						"name": "uuid",
						"type": [
							"null",
							{
								"type": "string",
								"logicalType": "uuid"
							}
						],
						"pgType": "uuid",
						"pgKey": false
					},
					{
						"name": "timestamp",
						"type": [
							"null",
							{
								"type": "long",
								"logicalType": "timestamp-micros"
							},
							{
								"name": "infinity_modifier",
								"symbols": ["infinity", "negative_infinity_ca5991f51367e3e4"],
								"type": "enum"
							}
						],
						"pgType": "timestamp",
						"pgKey": false
					},
					{
						"name": "date",
						"type": [
							"null",
							{
								"type": "int",
								"logicalType": "date"
							},
							"infinity_modifier"
						],
						"pgType": "date",
						"pgKey": false
					},
					{
						"name": "time",
						"type": [
							"null",
							{
								"type": "long",
								"logicalType": "time-micros"
							},
							"infinity_modifier"
						],
						"pgType": "time",
						"pgKey": false
					},
					{
						"name": "timestamptz",
						"type": [
							"null",
							{
								"type": "long",
								"logicalType": "timestamp-micros"
							},
							"infinity_modifier"
						],
						"pgType": "timestamptz",
						"pgKey": false
					},
					{
						"name": "numeric",
						"type": [
							"null",
							{
								"type": "bytes",
								"logicalType": "decimal",
								"precision": 23,
								"scale": 13
							}
						],
						"pgType": "numeric",
						"pgKey": false
					}
				]
			}`,
		}, {
			name: "replica identity full",
			input: &pglogrepl.RelationMessage{
				Namespace:       "public",
				RelationName:    "integration_tests",
				ReplicaIdentity: 'f',
				Columns: []*pglogrepl.RelationMessageColumn{
					{Flags: 1, Name: "id", DataType: pgtype.Int8OID},
					{Flags: 1, Name: "cool", DataType: pgtype.BoolOID},
					{Flags: 1, Name: "data", DataType: pgtype.JSONBOID},
				}},
			expected: `
			{
				"type": "record",
				"name": "integration_tests",
				"fields": [
					{
						"name": "id",
						"type": [
							"null",
							"long"
						],
						"pgType": "int8",
						"pgKey": true
					},
					{
						"name": "cool",
						"type": [
							"null",
							"boolean"
						],
						"pgType": "bool",
						"pgKey": true
					},
					{
						"name": "data",
						"type": [
							"null",
							"bytes"
						],
						"pgType": "jsonb",
						"pgKey": true
					}
				]
			}`,
		}, {
			name: "all keys",
			input: &pglogrepl.RelationMessage{
				RelationName:    "integration_tests",
				ReplicaIdentity: 'd',
				Columns: []*pglogrepl.RelationMessageColumn{
					{Flags: 1, Name: "id", DataType: pgtype.Int4OID},
					{Flags: 1, Name: "price", DataType: pgtype.Float8OID},
					{Flags: 1, Name: "less_accurate_price", DataType: pgtype.Float4OID},
				}},
			expected: `
			{
				"type": "record",
				"name": "integration_tests",
				"fields": [
					{
						"name": "id",
						"type": "int",
						"pgType": "int4",
						"pgKey": true
					},
					{
						"name": "price",
						"type": "double",
						"pgType": "float8",
						"pgKey": true
					},
					{
						"name": "less_accurate_price",
						"type": "float",
						"pgType": "float4",
						"pgKey": true
					}
				]
			}`,
		}, {
			name: "arrays",
			input: &pglogrepl.RelationMessage{
				RelationName:    "integration_tests",
				ReplicaIdentity: 'd',
				Columns: []*pglogrepl.RelationMessageColumn{
					{Flags: 1, Name: "ids", DataType: pgtype.Int4ArrayOID},
					{Flags: 0, Name: "prices", DataType: pgtype.Float8ArrayOID},
					{Flags: 0, Name: "names", DataType: pgtype.TextArrayOID},
				}},
			expected: `
			{
				"type": "record",
				"name": "integration_tests",
				"fields": [
					{
						"name": "ids",
						"type": {
							"type": "array",
							"items": "int"
						},
						"pgType": "_int4",
						"pgKey": true
					},
					{
						"name": "prices",
						"type": [
							"null",
							{
								"type": "array",
								"items": "double"
							}
						],
						"pgType": "_float8",
						"pgKey": false
					},
					{
						"name": "names",
						"type": [
							"null",
							{
								"type": "array",
								"items": "string"
							}
						],
						"pgType": "_text",
						"pgKey": false
					}
				]
			}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Get the actual result
			res, err := New(tc.input).RelationMessageToAvro()
			assert.NoError(t, err)

			// Marshal the actual result to JSON
			actualBytes, err := json.Marshal(res)
			assert.NoError(t, err)

			// Normalize expected JSON to remove whitespace and format consistently
			expectedNormalized := normalizeJSON(tc.expected)

			// Parse both as generic maps for comparison
			var expectedMap, actualMap map[string]interface{}

			err = json.Unmarshal([]byte(expectedNormalized), &expectedMap)
			assert.NoError(t, err, "Expected JSON should be valid")

			err = json.Unmarshal(actualBytes, &actualMap)
			assert.NoError(t, err, "Actual JSON should be valid")

			// Compare the structures
			assert.Equal(t, expectedMap, actualMap, "JSON structures should match for test case: "+tc.name)

			// Alternative direct JSON comparison (useful for debugging)
			// Normalize both to the same format
			actualNormalized := normalizeJSON(string(actualBytes))
			if expectedNormalized != actualNormalized {
				t.Logf("JSON formatting differs:\nExpected: %s\nActual: %s", expectedNormalized, actualNormalized)
			}
		})
	}
}

// normalizeJSON removes whitespace and formats the JSON string consistently
// to make it suitable for comparison
func normalizeJSON(jsonStr string) string {
	// Remove leading/trailing whitespace from each line
	lines := strings.Split(jsonStr, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}

	// Rejoin and parse the JSON to ensure valid format
	var buf bytes.Buffer
	err := json.Compact(&buf, []byte(strings.Join(lines, "")))
	if err != nil {
		// Return original if compact fails
		return jsonStr
	}

	return buf.String()
}
