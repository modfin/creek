package pgtypeavro

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Conversion from pgtypes to "native" types that the github.com/hamba/avro
// library expects.

func MapToNativeTypes(m map[string]any) (map[string]any, error) {
	r := make(map[string]any)
	for k, v := range m {
		v, err := pgtypeToNative(v)
		if err != nil {
			return r, err
		}
		r[k] = v
	}
	return r, nil
}

func pgtypeToNative(pgType any) (any, error) {
	switch t := pgType.(type) {
	case pgtype.Time:
		return time.Duration(t.Microseconds * 1000), nil
	case pgtype.UUID:
		s, _ := t.Value() // this function cannot fail
		return s, nil
	case pgtype.Date:
		if t.InfinityModifier == pgtype.Infinity {
			return pgtype.Infinity, nil
		} else if t.InfinityModifier == pgtype.NegativeInfinity {
			return pgtype.NegativeInfinity, nil
		}
		return t.Time, nil
	case pgtype.InfinityModifier:
		if t == pgtype.Infinity {
			return Infinity, nil
		}

		if t == pgtype.NegativeInfinity {
			return NegativeInfinity, nil
		}

		return t, errors.New("unexpected infinity modifier, is finite")
	case [16]uint8:
		// UUID
		return fmt.Sprintf("%x-%x-%x-%x-%x", t[0:4], t[4:6], t[6:8], t[8:10], t[10:16]), nil
	case pgtype.Numeric:
		rat := new(big.Rat).SetInt(t.Int)
		if t.Exp > 0 {
			mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.Exp)), nil)
			rat.Mul(rat, new(big.Rat).SetInt(mul))
			return rat, nil
		} else {
			mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-t.Exp)), nil)
			rat.Quo(rat, new(big.Rat).SetInt(mul))
			return rat, nil
		}
	case map[string]interface{}:
		// This is a JSON type
		b, err := json.Marshal(t)
		if err != nil {
			logrus.Errorf("failed to marshal map to json")
		}
		return b, nil
	case []interface{}:
		var ret []any
		for _, v := range t {
			native, err := pgtypeToNative(v)
			if err != nil {
				return native, err
			}
			ret = append(ret, native)
		}
		return ret, nil
	default:
		return pgType, nil
	}
}

func FoldWhile[I any, A any](slice []I, combined func(accumulator A, val I) (A, bool), init A) A {
	var cont bool
	for _, val := range slice {
		init, cont = combined(init, val)
		if !cont {
			return init
		}
	}
	return init
}
