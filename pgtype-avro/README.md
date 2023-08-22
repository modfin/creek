## pgtypeavro

A simple "lib" that makes allows for generating Avro schemas from a PostgreSQL
database. Specifically, it makes it possible to convert a
`pglogrepl.RelationMessage` into an Avro schema.

It has support for all primitive avro types, complex types, as well as the
following logical types:

* `decimal`
* `uuid`
* `date`
* `time-millis`
* `time-micros`
* `timestamp-micros`
* `local-timestamp-millis`
* `local-timestamp-micros`
* `duration`

It has support for the following limited set of PostgreSQL types, as well as the
array variants of these types:

* `bool`
* `char`
* `varchar`
* `bpchar`
* `date`
* `float4`
* `float8`
* `int2`
* `int4`
* `int8`
* `json`
* `jsonb`
* `text`
* `time`
* `timestamp`
* `timestamptz`
* `uuid`
* `numeric`

### Example usage

```go
package main

import (
	pgtypeavro "creek/pgtype-avro"
	"encoding/json"
	"log"

	"github.com/jackc/pglogrepl"
)

func main() {
	var relMsg pglogrepl.RelationMessage // A relation message from pglogrepl


	schema, err := pgtypeavro.New(relMsg).RelationMessageToAvro()
	if err != nil {
		log.Fatal(err)
	}

	bin, err := json.Marshal(schema)

	log.Println(string(bin))
}
```
