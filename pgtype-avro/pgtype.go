package pgtypeavro

import (
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/modfin/henry/slicez"
)

// NegativeInfinity Since avro names can't start with "-", we use a "magic" prefix.
// When decoding, this should be converted to -infinity.
const NegativeInfinity = "negative_infinity_ca5991f51367e3e4"
const Infinity = "infinity"

type PgToSchema interface {
	ToSchema() Schema
}

type PgType struct {
	Mapper PgToSchema
	Name   string
	OID    uint32
}

type AvroConverter struct {
	infinityExists bool

	rel *pglogrepl.RelationMessage
	tm  *SQLTypeMap
}

func New(rel *pglogrepl.RelationMessage) *AvroConverter {
	return &AvroConverter{
		infinityExists: false,
		rel:            rel,
		tm:             NewTypeMap(),
	}
}

func (a *AvroConverter) RelationMessageToAvro() (*Record, error) {
	a.infinityExists = false

	record := &Record{
		Type: TypeRecord,
		Name: a.rel.RelationName,
	}

	fields, err := a.columnsToAvro(a.rel.Columns)
	if err != nil {
		return nil, err
	}

	record.Fields = fields

	return record, nil
}

func (a *AvroConverter) RelationMessageKeysToAvro() (*Record, error) {
	a.infinityExists = false

	record := &Record{
		Type: TypeRecord,
		Name: a.rel.RelationName,
	}

	// Only primary keys
	cols := slicez.Filter(a.rel.Columns, func(c *pglogrepl.RelationMessageColumn) bool {
		return c.Flags == 1
	})

	fields, err := a.columnsToAvro(cols)
	if err != nil {
		return nil, err
	}

	record.Fields = fields

	return record, nil
}

func (a *AvroConverter) columnsToAvro(cols []*pglogrepl.RelationMessageColumn) ([]*RecordField, error) {
	var (
		err         error
		recordField *RecordField
	)

	fields := make([]*RecordField, 0)

	for _, col := range cols {
		t, ok := a.tm.TypeForOID(col.DataType)
		if !ok {
			t = &SQLType{
				Name:    "text",
				OID:     col.DataType,
				Variant: nil,
			}
		}
		recordField, err = a.typeToAvroField(col, t)
		if err != nil {
			return fields, err
		}

		fields = append(fields, recordField)
	}

	return fields, nil
}

func (a *AvroConverter) typeToAvroField(col *pglogrepl.RelationMessageColumn, t *SQLType) (*RecordField, error) {
	avroType, err := a.typeToAvroType(col, t)
	if err != nil {
		return nil, err
	}

	if a.rel.ReplicaIdentity != 'd' {
		// If the replica identity is not default, we need to make the type nullable
		switch t := avroType.(type) {
		case *Union:
			avroType = NewUnion(TypeNull, t.Members()...)
		default:
			avroType = Union{TypeNull, avroType}
		}

	} else if col.Flags == 0 {
		// If the column is not part of the primary key, we need to make the type nullable
		switch t := avroType.(type) {
		case *Union:
			avroType = NewUnion(TypeNull, t.Members()...)
		default:
			avroType = Union{TypeNull, avroType}
		}
	}

	var isKey = new(bool)
	*isKey = col.Flags == 1

	return &RecordField{
		Name:   col.Name,
		Type:   avroType,
		PgType: t.Name,
		PgKey:  isKey,
	}, nil
}

func (a *AvroConverter) getInf() (inf Schema) {
	if a.infinityExists {
		inf = Type("infinity_modifier")
	} else {
		inf = &Enum{
			Type:    TypeEnum,
			Name:    "infinity_modifier",
			Symbols: []string{Infinity, NegativeInfinity},
		}
		a.infinityExists = true
	}
	return
}

func (a *AvroConverter) typeToAvroType(col *pglogrepl.RelationMessageColumn, t *SQLType) (Schema, error) {

	if t.Variant == nil {
		switch t {
		case &Bool:
			return TypeBool, nil
		case &Char, &VarChar, &Text, &BPChar:
			return TypeStr, nil
		case &Float4:
			return TypeFloat, nil
		case &Float8:
			return TypeDouble, nil
		case &Int2, &Int4:
			return TypeInt, nil
		case &Int8:
			return TypeLong, nil
		case &Date:
			return &Union{
				&DerivedType{
					Type:        TypeInt,
					LogicalType: LogicalTypeDate,
				},
				a.getInf(),
			}, nil
		case &Time:
			return &Union{
				&DerivedType{
					Type:        TypeLong,
					LogicalType: LogicalTypeTimeMicros,
				},
				a.getInf(),
			}, nil

		case &Timestamp, &Timestamptz:
			return &Union{
				&DerivedType{
					Type:        TypeLong,
					LogicalType: LogicalTypeTimestampMicros,
				},
				a.getInf(),
			}, nil
		case &UUID:
			return &DerivedType{
				Type:        TypeStr,
				LogicalType: LogicalTypeUUID,
			}, nil
		case &JSONB, &JSON:
			return TypeBytes, nil
		case &Numeric:
			precision, scale := getNumericAttrs(col.TypeModifier)

			derived := &DerivedType{
				Type:        TypeBytes,
				LogicalType: LogicalTypeDecimal,
				Precision:   int(precision),
				Scale:       int(scale),
			}

			return derived, nil
		default:
			return TypeStr, nil
			// Limitation: we only support the types above
		}
	} else if t.Variant.Kind == SQLTypeArray {
		innerType, ok := a.tm.TypeForOID(t.Variant.ItemOID)
		if !ok {
			return nil, fmt.Errorf("unknown type for oid: %d", t.Variant.ItemOID)
		}
		innerItemType, err := a.typeToAvroType(col, innerType)
		if err != nil {
			return nil, err
		}
		return &Array{Type: TypeArray, Items: innerItemType}, nil
	} else {
		return nil, fmt.Errorf("unsupported type: %s", t.Name)
	}

}

func getNumericAttrs(typeMod int32) (precision int32, scale int32) {
	// Gets the scale and precision based on the type modifier See source code
	// from
	// https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/numeric.c#L897
	// Upper 16 bits are precision Lower 16 bits are used for for scale, only
	// the lower 11 bits of those are used however For historical reasons
	// VARHDRZ (4 bytes) is added ¯\_(ツ)_/¯

	return ((typeMod - 4) >> 16) & 0xffff, ((typeMod-4)&0x7ff ^ 1024) - 1024
}
