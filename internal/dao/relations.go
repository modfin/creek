package dao

import (
	"github.com/jackc/pglogrepl"
	pgtypeavro "github.com/modfin/creek/pgtype-avro"
)

// Idk, maybe this will be useful in future to have some wrapper functions around relations
type Relation struct {
	fingerprint string
	Msg         *pglogrepl.RelationMessage
	Schema      pgtypeavro.Schema // Schema for all fields
	KeySchema   pgtypeavro.Schema // Schema for key fields
}
