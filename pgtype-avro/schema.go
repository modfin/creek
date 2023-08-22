package pgtypeavro

type Type string

const (
	// Primitive types
	TypeNull   Type = "null"
	TypeBool   Type = "boolean"
	TypeInt    Type = "int"
	TypeLong   Type = "long"
	TypeFloat  Type = "float"
	TypeDouble Type = "double"

	TypeBytes Type = "bytes"
	TypeStr   Type = "string"

	// Complex types
	TypeRecord Type = "record"
	TypeEnum   Type = "enum"
	TypeArray  Type = "array"
	TypeMap    Type = "map"
	TypeUnion  Type = "union"
	TypeFixed  Type = "fixed"
)

type LogicalType Type

const (
	LogicalTypeDecimal              LogicalType = "decimal"
	LogicalTypeUUID                 LogicalType = "uuid"
	LogicalTypeDate                 LogicalType = "date"
	LogicalTypeTimeMillis           LogicalType = "time-millis"
	LogicalTypeTimeMicros           LogicalType = "time-micros"
	LogicalTypeTimestampMicros      LogicalType = "timestamp-micros"
	LogicalTypeLocalTimestampMillis LogicalType = "local-timestamp-millis"
	LogicalTypeLocalTimestampMicros LogicalType = "local-timestamp-micros"
	LogicalTypeDuration             LogicalType = "duration"
)

type Schema interface {
	TypeName() Type
}

func (t Type) TypeName() Type {
	return t
}

type Array struct {
	Type    Type   `json:"type"`
	Items   Schema `json:"items"`
	Default []any  `json:"default,omitempty"`
	PgType  string `json:"pgType,omitempty"`
}

func (a *Array) TypeName() Type {
	return TypeArray
}

type DerivedType struct {
	Type        Type        `json:"type"`
	LogicalType LogicalType `json:"logicalType"`
	Precision   int         `json:"precision,omitempty"`
	Scale       int         `json:"scale,omitempty"`
}

func (t *DerivedType) TypeName() Type {
	return t.Type
}

type Record struct {
	Type      Type           `json:"type"`
	Name      string         `json:"name"`
	Namespace string         `json:"namespace,omitempty"`
	Doc       string         `json:"doc,omitempty"`
	Aliases   []string       `json:"aliases,omitempty"`
	Fields    []*RecordField `json:"fields"`
}

type RecordField struct {
	Name    string   `json:"name"`
	Doc     string   `json:"doc,omitempty"`
	Type    Schema   `json:"type"`
	Default any      `json:"default,omitempty"`
	Order   Order    `json:"order,omitempty"`
	Aliases []string `json:"aliases,omitempty"`
	PgType  string   `json:"pgType,omitempty"`
	PgKey   *bool    `json:"pgKey,omitempty"`
}

type Order string

const (
	OrderAscending  Order = "ascending"
	OrderDescending Order = "descending"
	OrderIgnore     Order = "ignore"
)

func (r *Record) TypeName() Type {
	return TypeRecord
}

type Enum struct {
	Type      Type     `json:"type"`
	Name      string   `json:"name"`
	Namespace string   `json:"namespace,omitempty"`
	Doc       string   `json:"doc,omitempty"`
	Symbols   []string `json:"symbols"`
	Default   string   `json:"default,omitempty"`
	Aliases   []string `json:"aliases,omitempty"`
}

func (e *Enum) TypeName() Type {
	return TypeEnum
}

type Union []Schema

func (u Union) TypeName() Type {
	return TypeUnion
}

func (u Union) Members() []Schema {
	return u
}

func NewUnion(schema Schema, schemas ...Schema) Union {
	u := Union{schema}
	for _, s := range schemas {
		u = append(u, s)
	}
	return u
}
