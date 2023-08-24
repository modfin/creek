package pgtypeavro

import (
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestTimeConversion(t *testing.T) {
	input := pgtype.Time{
		Microseconds: 123,
		Valid:        true,
	}

	output, err := pgtypeToNative(input)

	assert.NoError(t, err)

	assert.Equal(t, time.Microsecond*123, output)
}

func TestUUIDConversion(t *testing.T) {
	input := pgtype.UUID{
		Bytes: [16]byte{0x5, 0x96, 0xd0, 0x4b, 0x74, 0x95, 0x43, 0x31, 0x9a, 0xca, 0x54, 0xd3, 0xcb, 0xe0, 0xdd, 0x31},
		Valid: true,
	}

	output, err := pgtypeToNative(input)

	assert.NoError(t, err)

	assert.Equal(t, "0596d04b-7495-4331-9aca-54d3cbe0dd31", output)
}

func TestUUIDConversionBytes(t *testing.T) {
	// Sometimes/always? the data is parsed to [16]byte instead of pgtype.UUID
	input := [16]byte{0x60, 0xa4, 0x18, 0x51, 0x25, 0xf7, 0x41, 0x14, 0xb0, 0x73, 0x7f, 0x23, 0x96, 0xbf, 0x84, 0x5b}
	output, err := pgtypeToNative(input)

	assert.NoError(t, err)

	assert.Equal(t, "60a41851-25f7-4114-b073-7f2396bf845b", output)
}

func TestNumericConversion(t *testing.T) {
	var num pgtype.Numeric
	err := num.Scan("0.00001")
	assert.NoError(t, err)

	tests := []struct {
		name     string
		input    pgtype.Numeric
		expected *big.Rat
	}{
		{
			name:     "positive exponent",
			input:    pgtype.Numeric{Int: big.NewInt(2329), Exp: 4, Valid: true},
			expected: big.NewRat(23290000, 1),
		},
		{
			name:     "negative exponent",
			input:    pgtype.Numeric{Int: big.NewInt(732), Exp: -2, Valid: true},
			expected: big.NewRat(732, 100),
		},
		{
			name: "zero exponent",
			input: pgtype.Numeric{
				Int: big.NewInt(5792), Exp: 0, Valid: true},
			expected: big.NewRat(5792, 1),
		},
		{
			name:     "zero",
			input:    pgtype.Numeric{Int: big.NewInt(0), Exp: 0, Valid: true},
			expected: big.NewRat(0, 1),
		},
		{
			name:     "big number",
			input:    pgtype.Numeric{Int: big.NewInt(1213), Exp: 10, Valid: true},
			expected: big.NewRat(12130000000000, 1),
		},
		{
			name:     "exponent",
			input:    num,
			expected: big.NewRat(1, 100000),
		},
		{
			name:     "failing",
			input:    pgtype.Numeric{Int: big.NewInt(23111200), Exp: -5, Valid: true},
			expected: big.NewRat(23111200, 100000),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output, err := pgtypeToNative(tc.input)
			rat := output.(*big.Rat)
			assert.NoError(t, err)

			assert.Zero(t, rat.Cmp(tc.expected))
		})
	}

}

func TestUUIDConversionJSON(t *testing.T) {
	input := map[string]interface{}{
		"integration_tests": 23,
		"css_selector":      "body > div.container",
	}
	output, err := pgtypeToNative(input)

	assert.NoError(t, err)

	assert.Equal(t, []byte("{\"css_selector\":\"body \\u003e div.container\",\"integration_tests\":23}"), output)
}
