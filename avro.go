package avrosqlite

import (
	"errors"

	"github.com/hamba/avro"
)

var (
	NullSchema   = avro.MustParse(`{"type": "null"}`)
	LongSchema   = avro.MustParse(`{"type": "long"}`)
	DoubleSchema = avro.MustParse(`{"type": "double"}`)
	StringSchema = avro.MustParse(`{"type": "string"}`)
	BytesSchema  = avro.MustParse(`{"type": "bytes"}`)
)

// AvroToSqliteSchema returns the sqlite schema for the avro schema
func AvroToSqliteSchema(schema avro.Schema) (*SqliteSchema, error) {
	return nil, errors.New("not implemented")
}

// SqliteToAvroType converts a sqlite type to an avro type.
// Sqlite typoes are convered into the largest avro type that can hold the sqlite type.
// This means that representations are not as dense as they could be, but it is a simple
// way to ensure compatibility.
// https://www.sqlite.org/datatype3.html
// https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive
func SqliteToAvroType(sqliteType SQLiteType) (avro.Type, error) {
	switch sqliteType {
	case SQLiteNull:
		return avro.Null, nil
	case SQLiteInteger:
		return avro.Long, nil
	case SQLiteReal:
		return avro.Double, nil
	case SQLiteText:
		return avro.String, nil
	case SQLiteBlob:
		return avro.Bytes, nil
	default:
		return "", errors.New("unknown sqlite type")
	}
}

func SqliteToAvroSchema(sqliteType SQLiteType) (avro.Schema, error) {
	switch sqliteType {
	case SQLiteNull:
		return NullSchema, nil
	case SQLiteInteger:
		return LongSchema, nil
	case SQLiteReal:
		return DoubleSchema, nil
	case SQLiteText:
		return StringSchema, nil
	case SQLiteBlob:
		return BytesSchema, nil
	default:
		return nil, errors.New("unknown sqlite type")
	}
}
