package avrosqlite

import (
	"errors"

	"github.com/hamba/avro"
)

var (
	nullSchema   = avro.MustParse(`{"type": "null"}`)
	longSchema   = avro.MustParse(`{"type": "long"}`)
	doubleSchema = avro.MustParse(`{"type": "double"}`)
	stringSchema = avro.MustParse(`{"type": "string"}`)
	bytesSchema  = avro.MustParse(`{"type": "bytes"}`)
)

// AvroToSqliteSchema returns the sqlite schema for the avro schema
func AvroToSqliteSchema(schema avro.Schema) (*SqliteSchema, error) {
	return nil, errors.New("not implemented")
}

// sqliteToAvroSchema converts a sqlite type to an avro primitve schema.
// Sqlite typoes are convered into the largest avro type that can hold the sqlite type.
// This means that representations are not as dense as they could be, but it is a simple
// way to ensure compatibility.
// https://www.sqlite.org/datatype3.html
// https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive
func sqliteToAvroSchema(t sqliteType) (avro.Schema, error) {
	switch t {
	case sqliteNull:
		return nullSchema, nil
	case sqliteInteger:
		return longSchema, nil
	case sqliteReal:
		return doubleSchema, nil
	case sqliteText:
		return stringSchema, nil
	case sqliteBlob:
		return bytesSchema, nil
	default:
		return nil, errors.New("unknown sqlite type")
	}
}
