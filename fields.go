// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"database/sql"
	"reflect"
	"time"
)

var typeDatabaseName = map[fieldType]string{
	fieldTypeDecimal:    "DECIMAL",
	fieldTypeTiny:       "TINYINT",
	fieldTypeShort:      "SMALLINT",
	fieldTypeLong:       "INT",
	fieldTypeFloat:      "FLOAT",
	fieldTypeDouble:     "DOUBLE",
	fieldTypeNULL:       "NULL",
	fieldTypeTimestamp:  "TIMESTAMP",
	fieldTypeLongLong:   "BIGINT",
	fieldTypeInt24:      "MEDIUMINT",
	fieldTypeDate:       "DATE",
	fieldTypeTime:       "TIME",
	fieldTypeDateTime:   "DATETIME",
	fieldTypeYear:       "YEAR",
	fieldTypeNewDate:    "DATE",
	fieldTypeVarChar:    "VARCHAR",
	fieldTypeBit:        "BIT",
	fieldTypeJSON:       "JSON",
	fieldTypeNewDecimal: "DECIMAL",
	fieldTypeEnum:       "ENUM",
	fieldTypeSet:        "SET",
	fieldTypeTinyBLOB:   "TINYBLOB",
	fieldTypeMediumBLOB: "MEDIUMBLOB",
	fieldTypeLongBLOB:   "LONGBLOB",
	fieldTypeBLOB:       "BLOB",
	fieldTypeVarString:  "VARSTRING", // correct?
	fieldTypeString:     "STRING",    // correct?
	fieldTypeGeometry:   "GEOMETRY",
}

var (
	scanTypeNil        = reflect.TypeOf(nil)
	scanTypeNullInt    = reflect.TypeOf(sql.NullInt64{})
	scanTypeUint8      = reflect.TypeOf(uint8(0))
	scanTypeInt8       = reflect.TypeOf(int8(0))
	scanTypeUint16     = reflect.TypeOf(uint16(0))
	scanTypeInt16      = reflect.TypeOf(int16(0))
	scanTypeUint32     = reflect.TypeOf(uint32(0))
	scanTypeInt32      = reflect.TypeOf(int32(0))
	scanTypeUint64     = reflect.TypeOf(uint64(0))
	scanTypeInt64      = reflect.TypeOf(int64(0))
	scanTypeNullFloat  = reflect.TypeOf(sql.NullFloat64{})
	scanTypeFloat32    = reflect.TypeOf(float32(0))
	scanTypeFloat64    = reflect.TypeOf(float64(0))
	scanTypeNullString = reflect.TypeOf(sql.NullString{})
	scanTypeString     = reflect.TypeOf("")
	scanTypeBytes      = reflect.TypeOf([]byte{})
	scanTypeRawBytes   = reflect.TypeOf(sql.RawBytes{})
	scanTypeTime       = reflect.TypeOf(time.Time{})
	scanTypeNullTime   = reflect.TypeOf(NullTime{})
	scanTypeUnknown    = reflect.TypeOf(new(interface{}))
)

type mysqlField struct {
	tableName string
	name      string
	flags     fieldFlag
	fieldType fieldType
	decimals  byte
}

func (mf *mysqlField) scanType() reflect.Type {
	switch mf.fieldType {
	case fieldTypeNULL:
		return scanTypeNil

	case fieldTypeTiny:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint8
			}
			return scanTypeInt8
		}
		return scanTypeNullInt

	case fieldTypeShort, fieldTypeYear:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint16
			}
			return scanTypeInt16
		}
		return scanTypeNullInt

	case fieldTypeInt24, fieldTypeLong:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint32
			}
			return scanTypeInt32
		}
		return scanTypeNullInt

	case fieldTypeLongLong:
		if mf.flags&flagNotNULL != 0 {
			if mf.flags&flagUnsigned != 0 {
				return scanTypeUint64
			}
			return scanTypeInt64
		}
		return scanTypeNullInt

	case fieldTypeFloat:
		if mf.flags&flagNotNULL != 0 {
			return scanTypeFloat32
		}
		return scanTypeNullFloat

	case fieldTypeDouble:
		if mf.flags&flagNotNULL != 0 {
			return scanTypeFloat64
		}
		return scanTypeNullFloat

	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
		fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
		fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON,
		fieldTypeTime:
		if mf.flags&flagNotNULL != 0 {
			// alternatively we could return []byte or even RawBytes
			return scanTypeString
		}
		return scanTypeNullString

	case fieldTypeDate, fieldTypeNewDate,
		fieldTypeTimestamp, fieldTypeDateTime:

		// TODO: respect rows.mc.parseTime

		if mf.flags&flagNotNULL != 0 {
			return scanTypeTime
		}
		return scanTypeNullTime

	default:
		return scanTypeUnknown
	}
}
