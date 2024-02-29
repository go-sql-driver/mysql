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
)

func (mf *MysqlField) TypeDatabaseName() string {
	switch mf.FieldType {
	case fieldTypeBit:
		return "BIT"
	case fieldTypeBLOB:
		if mf.Charset != binaryCollationID {
			return "TEXT"
		}
		return "BLOB"
	case fieldTypeDate:
		return "DATE"
	case fieldTypeDateTime:
		return "DATETIME"
	case fieldTypeDecimal:
		return "DECIMAL"
	case fieldTypeDouble:
		return "DOUBLE"
	case fieldTypeEnum:
		return "ENUM"
	case fieldTypeFloat:
		return "FLOAT"
	case fieldTypeGeometry:
		return "GEOMETRY"
	case fieldTypeInt24:
		if mf.Flags&flagUnsigned != 0 {
			return "UNSIGNED MEDIUMINT"
		}
		return "MEDIUMINT"
	case fieldTypeJSON:
		return "JSON"
	case fieldTypeLong:
		if mf.Flags&flagUnsigned != 0 {
			return "UNSIGNED INT"
		}
		return "INT"
	case fieldTypeLongBLOB:
		if mf.Charset != binaryCollationID {
			return "LONGTEXT"
		}
		return "LONGBLOB"
	case fieldTypeLongLong:
		if mf.Flags&flagUnsigned != 0 {
			return "UNSIGNED BIGINT"
		}
		return "BIGINT"
	case fieldTypeMediumBLOB:
		if mf.Charset != binaryCollationID {
			return "MEDIUMTEXT"
		}
		return "MEDIUMBLOB"
	case fieldTypeNewDate:
		return "DATE"
	case fieldTypeNewDecimal:
		return "DECIMAL"
	case fieldTypeNULL:
		return "NULL"
	case fieldTypeSet:
		return "SET"
	case fieldTypeShort:
		if mf.Flags&flagUnsigned != 0 {
			return "UNSIGNED SMALLINT"
		}
		return "SMALLINT"
	case fieldTypeString:
		if mf.Flags&flagEnum != 0 {
			return "ENUM"
		} else if mf.Flags&flagSet != 0 {
			return "SET"
		}
		if mf.Charset == binaryCollationID {
			return "BINARY"
		}
		return "CHAR"
	case fieldTypeTime:
		return "TIME"
	case fieldTypeTimestamp:
		return "TIMESTAMP"
	case fieldTypeTiny:
		if mf.Flags&flagUnsigned != 0 {
			return "UNSIGNED TINYINT"
		}
		return "TINYINT"
	case fieldTypeTinyBLOB:
		if mf.Charset != binaryCollationID {
			return "TINYTEXT"
		}
		return "TINYBLOB"
	case fieldTypeVarChar:
		if mf.Charset == binaryCollationID {
			return "VARBINARY"
		}
		return "VARCHAR"
	case fieldTypeVarString:
		if mf.Charset == binaryCollationID {
			return "VARBINARY"
		}
		return "VARCHAR"
	case fieldTypeYear:
		return "YEAR"
	default:
		return ""
	}
}

var (
	scanTypeFloat32    = reflect.TypeOf(float32(0))
	scanTypeFloat64    = reflect.TypeOf(float64(0))
	scanTypeInt8       = reflect.TypeOf(int8(0))
	scanTypeInt16      = reflect.TypeOf(int16(0))
	scanTypeInt32      = reflect.TypeOf(int32(0))
	scanTypeInt64      = reflect.TypeOf(int64(0))
	scanTypeNullFloat  = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt    = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullTime   = reflect.TypeOf(sql.NullTime{})
	scanTypeUint8      = reflect.TypeOf(uint8(0))
	scanTypeUint16     = reflect.TypeOf(uint16(0))
	scanTypeUint32     = reflect.TypeOf(uint32(0))
	scanTypeUint64     = reflect.TypeOf(uint64(0))
	scanTypeString     = reflect.TypeOf("")
	scanTypeNullString = reflect.TypeOf(sql.NullString{})
	scanTypeBytes      = reflect.TypeOf([]byte{})
	scanTypeUnknown    = reflect.TypeOf(new(interface{}))
)

type MysqlField struct {
	TableName string    `json:"table_name"`
	Name      string    `json:"name"`
	Length    uint32    `json:"length"`
	Flags     fieldFlag `json:"flags"`
	FieldType fieldType `json:"field_type"`
	Decimals  byte      `json:"decimals"`
	Charset   uint8     `json:"charset"`
}

func (mf *MysqlField) scanType() reflect.Type {
	switch mf.FieldType {
	case fieldTypeTiny:
		if mf.Flags&flagNotNULL != 0 {
			if mf.Flags&flagUnsigned != 0 {
				return scanTypeUint8
			}
			return scanTypeInt8
		}
		return scanTypeNullInt

	case fieldTypeShort, fieldTypeYear:
		if mf.Flags&flagNotNULL != 0 {
			if mf.Flags&flagUnsigned != 0 {
				return scanTypeUint16
			}
			return scanTypeInt16
		}
		return scanTypeNullInt

	case fieldTypeInt24, fieldTypeLong:
		if mf.Flags&flagNotNULL != 0 {
			if mf.Flags&flagUnsigned != 0 {
				return scanTypeUint32
			}
			return scanTypeInt32
		}
		return scanTypeNullInt

	case fieldTypeLongLong:
		if mf.Flags&flagNotNULL != 0 {
			if mf.Flags&flagUnsigned != 0 {
				return scanTypeUint64
			}
			return scanTypeInt64
		}
		return scanTypeNullInt

	case fieldTypeFloat:
		if mf.Flags&flagNotNULL != 0 {
			return scanTypeFloat32
		}
		return scanTypeNullFloat

	case fieldTypeDouble:
		if mf.Flags&flagNotNULL != 0 {
			return scanTypeFloat64
		}
		return scanTypeNullFloat

	case fieldTypeBit, fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeLongBLOB,
		fieldTypeBLOB, fieldTypeVarString, fieldTypeString, fieldTypeGeometry:
		if mf.Charset == binaryCollationID {
			return scanTypeBytes
		}
		fallthrough
	case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
		fieldTypeEnum, fieldTypeSet, fieldTypeJSON, fieldTypeTime:
		if mf.Flags&flagNotNULL != 0 {
			return scanTypeString
		}
		return scanTypeNullString

	case fieldTypeDate, fieldTypeNewDate,
		fieldTypeTimestamp, fieldTypeDateTime:
		// NullTime is always returned for more consistent behavior as it can
		// handle both cases of parseTime regardless if the field is nullable.
		return scanTypeNullTime

	default:
		return scanTypeUnknown
	}
}
