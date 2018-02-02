// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestValueThatIsValue(t *testing.T) {
	now := time.Now()
	inputs := []interface{}{nil, float64(1.0), int64(17), "ABC", now}

	for _, in := range inputs {
		out, err := converter{}.ConvertValue(in)
		if err != nil {
			t.Fatalf("Value %#v %T not needing conversion caused error: %s", in, in, err)
		}
		if out != in {
			t.Fatalf("Value %#v %T altered in conversion got %#v %T", in, in, out, out)
		}
	}
}

func TestValueThatIsPtrToValue(t *testing.T) {
	w := "ABC"
	x := &w
	y := &x
	inputs := []interface{}{x, y}

	for _, in := range inputs {
		out, err := converter{}.ConvertValue(in)
		if err != nil {
			t.Fatalf("Pointer %#v %T to value not needing conversion caused error: %s", in, in, err)
		}
		if out != w {
			t.Fatalf("Value %#v %T not resolved to string in conversion (got %#v %T)", in, in, out, out)
		}
	}
}

func TestValueThatIsTypedPtrToNil(t *testing.T) {
	var w *string
	x := &w
	y := &x
	inputs := []interface{}{x, y}

	for _, in := range inputs {
		out, err := converter{}.ConvertValue(in)
		if err != nil {
			t.Fatalf("Pointer %#v %T to nil value caused error: %s", in, in, err)
		}
		if out != nil {
			t.Fatalf("Pointer to nil did not Value as nil")
		}
	}
}

type implementsValuer uint64

func (me implementsValuer) Value() (driver.Value, error) {
	return string(me), nil
}
func TestTypesThatImplementValuerAreSkipped(t *testing.T) {
	// Have to test on a uint64 with high bit set - as we skip everything else anyhow
	x := implementsValuer(^uint64(0))
	y := &x
	z := &y
	var a *implementsValuer
	b := &a
	c := &b
	inputs := []interface{}{x, y, z, a, b, c}

	for _, in := range inputs {
		_, err := converter{}.ConvertValue(in)
		if err != driver.ErrSkip {
			t.Fatalf("Conversion of Valuer implementing type %T not skipped", in)
		}
	}
}

func TestTypesThatAreNotValuesAreSkipped(t *testing.T) {
	type derived1 string  // convertable
	type derived2 []uint8 // convertable
	type derived3 []int   // not convertable
	type derived4 uint64  // without the high bit set
	inputs := []interface{}{derived1("ABC"), derived2([]uint8{'A', 'B'}), derived3([]int{17, 32}), derived3(nil), derived4(26)}

	for _, in := range inputs {
		_, err := converter{}.ConvertValue(in)
		if err != driver.ErrSkip {
			t.Fatalf("Conversion of non-value value %#v %T not skipped", in, in)
		}
	}
}

func TestConvertLargeUnsignedIntegers(t *testing.T) {
	type derived uint64
	type derived2 *uint64
	v := ^uint64(0)
	w := &v
	x := derived(v)
	y := &x
	z := derived2(w)

	inputs := []interface{}{v, w, x, y, z}

	for _, in := range inputs {
		out, err := converter{}.ConvertValue(in)
		if err != nil {
			t.Fatalf("uint64 high-bit not convertible for type %T", in)
		}
		if out != "18446744073709551615" {
			t.Fatalf("uint64 high-bit not converted, got %#v %T", out, out)
		}
	}
}
