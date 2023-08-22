package pgtypeavro

import (
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
)

type SQLTypeKind string

// Only array types are supported at the moment
const (
	SQLTypeArray      SQLTypeKind = "array"
	SQLTypeRange      SQLTypeKind = "range"
	SQLTypeMultirange SQLTypeKind = "multirange"
)

type SQLTypeMap struct {
	// Map of PostgreSQL type OID to SQLType.
	oidToType map[uint32]*SQLType
}

type SQLTypeVariant struct {
	Kind    SQLTypeKind
	ItemOID uint32
}

type SQLType struct {
	Name    string
	OID     uint32
	Variant *SQLTypeVariant
}

var (
	// defaultMap contains default mappings between PostgreSQL server types and Go type handling logic.
	defaultMap         *SQLTypeMap
	defaultMapInitOnce = sync.Once{}

	/// Base types
	Bool        = SQLType{Name: "bool", OID: pgtype.BoolOID}
	Char        = SQLType{Name: "char", OID: pgtype.QCharOID}
	VarChar     = SQLType{Name: "varchar", OID: pgtype.VarcharOID}
	BPChar      = SQLType{Name: "bpchar", OID: pgtype.BPCharOID}
	Date        = SQLType{Name: "date", OID: pgtype.DateOID}
	Float4      = SQLType{Name: "float4", OID: pgtype.Float4OID}
	Float8      = SQLType{Name: "float8", OID: pgtype.Float8OID}
	Int2        = SQLType{Name: "int2", OID: pgtype.Int2OID}
	Int4        = SQLType{Name: "int4", OID: pgtype.Int4OID}
	Int8        = SQLType{Name: "int8", OID: pgtype.Int8OID}
	JSON        = SQLType{Name: "json", OID: pgtype.JSONOID}
	JSONB       = SQLType{Name: "jsonb", OID: pgtype.JSONBOID}
	Text        = SQLType{Name: "text", OID: pgtype.TextOID}
	Time        = SQLType{Name: "time", OID: pgtype.TimeOID}
	Timestamp   = SQLType{Name: "timestamp", OID: pgtype.TimestampOID}
	Timestamptz = SQLType{Name: "timestamptz", OID: pgtype.TimestamptzOID}
	UUID        = SQLType{Name: "uuid", OID: pgtype.UUIDOID}
	Numeric     = SQLType{Name: "numeric", OID: pgtype.NumericOID}

	/// Array types
	BoolArray        = SQLType{Name: "_bool", OID: pgtype.BoolArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.BoolOID}}
	CharArray        = SQLType{Name: "_char", OID: pgtype.QCharArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.QCharOID}}
	VarCharArray     = SQLType{Name: "_varchar", OID: pgtype.VarcharArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.VarcharOID}}
	BPCharArray      = SQLType{Name: "_bpchar", OID: pgtype.BPCharArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.BPCharOID}}
	DateArray        = SQLType{Name: "_date", OID: pgtype.DateArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.DateOID}}
	Float4Array      = SQLType{Name: "_float4", OID: pgtype.Float4ArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.Float4OID}}
	Float8Array      = SQLType{Name: "_float8", OID: pgtype.Float8ArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.Float8OID}}
	Int2Array        = SQLType{Name: "_int2", OID: pgtype.Int2ArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.Int2OID}}
	Int4Array        = SQLType{Name: "_int4", OID: pgtype.Int4ArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.Int4OID}}
	Int8Array        = SQLType{Name: "_int8", OID: pgtype.Int8ArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.Int8OID}}
	JSONArray        = SQLType{Name: "_json", OID: pgtype.JSONArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.JSONOID}}
	JSONBArray       = SQLType{Name: "_jsonb", OID: pgtype.JSONBArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.JSONBOID}}
	TextArray        = SQLType{Name: "_text", OID: pgtype.TextArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.TextOID}}
	TimeArray        = SQLType{Name: "_time", OID: pgtype.TimeArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.TimeOID}}
	TimestampArray   = SQLType{Name: "_timestamp", OID: pgtype.TimestampArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.TimestampOID}}
	TimestamptzArray = SQLType{Name: "_timestamptz", OID: pgtype.TimestamptzArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.TimestamptzOID}}
	UUIDArray        = SQLType{Name: "_uuid", OID: pgtype.UUIDArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.UUIDOID}}
	NumericArray     = SQLType{Name: "_numeric", OID: pgtype.NumericArrayOID, Variant: &SQLTypeVariant{Kind: SQLTypeArray, ItemOID: pgtype.NumericOID}}
)

func (tm *SQLTypeMap) RegisterType(t *SQLType) {
	tm.oidToType[t.OID] = t
}

func NewTypeMap() *SQLTypeMap {
	defaultMapInitOnce.Do(initDefaultMap)
	return defaultMap
}

func (tm *SQLTypeMap) TypeForOID(oid uint32) (*SQLType, bool) {
	t, ok := tm.oidToType[oid]
	return t, ok
}

func initDefaultMap() {
	defaultMap = &SQLTypeMap{
		oidToType: make(map[uint32]*SQLType),
	}

	defaultMap.RegisterType(&Bool)
	defaultMap.RegisterType(&Char)
	defaultMap.RegisterType(&VarChar)
	defaultMap.RegisterType(&BPChar)
	defaultMap.RegisterType(&Date)
	defaultMap.RegisterType(&Float4)
	defaultMap.RegisterType(&Float8)
	defaultMap.RegisterType(&Int2)
	defaultMap.RegisterType(&Int4)
	defaultMap.RegisterType(&Int8)
	defaultMap.RegisterType(&JSON)
	defaultMap.RegisterType(&JSONB)
	defaultMap.RegisterType(&Text)
	defaultMap.RegisterType(&Time)
	defaultMap.RegisterType(&Timestamp)
	defaultMap.RegisterType(&Timestamptz)
	defaultMap.RegisterType(&UUID)
	defaultMap.RegisterType(&Numeric)

	defaultMap.RegisterType(&BoolArray)
	defaultMap.RegisterType(&CharArray)
	defaultMap.RegisterType(&VarCharArray)
	defaultMap.RegisterType(&BPCharArray)
	defaultMap.RegisterType(&DateArray)
	defaultMap.RegisterType(&Float4Array)
	defaultMap.RegisterType(&Float8Array)
	defaultMap.RegisterType(&Int2Array)
	defaultMap.RegisterType(&Int4Array)
	defaultMap.RegisterType(&Int8Array)
	defaultMap.RegisterType(&JSONArray)
	defaultMap.RegisterType(&JSONBArray)
	defaultMap.RegisterType(&TextArray)
	defaultMap.RegisterType(&TimeArray)
	defaultMap.RegisterType(&TimestampArray)
	defaultMap.RegisterType(&TimestamptzArray)
	defaultMap.RegisterType(&UUIDArray)
	defaultMap.RegisterType(&NumericArray)
}

/* None of these types are currently supported:

* Indicates that this might be useful

Basic types:

aclitem
bit
box
bpchar
bytea       *
cid
cidr
circle
inet       *
interval   *
jsonpath
line
lseg
macaddr
name
oid
path
point
polygon
record
tid
varbit
xid

Range types:  *

daterange
int4range
int8range
numrange
tsrange
tstzrange

// Multirange types

datemultirange
int4multirange
int8multirange
nummultirange
tsmultirange
tstzmultirange

// Array types
_aclitem
_bit
_box
_bytea
_cid
_cidr
_circle
_daterange
_inet
_interval
_int4range
_int8range
_interval
_jsonpath
_line
_lseg
_macaddr
_name
_numrange
_oid
_path
_point
_polygon
_record
_tid
_tsrange
_tstzrange
_varbit
_xid
*/
