package avrosqlite

import (
	"errors"
	"fmt"
	"io"

	"github.com/hamba/avro"
)

var (
	nullSchema    = avro.MustParse(`{"type": "null"}`)
	longSchema    = avro.MustParse(`{"type": "long"}`)
	doubleSchema  = avro.MustParse(`{"type": "double"}`)
	stringSchema  = avro.MustParse(`{"type": "string"}`)
	bytesSchema   = avro.MustParse(`{"type": "bytes"}`)
	booleanSchema = avro.MustParse(`{"type": "boolean"}`)
)

// AvroToSqliteSchema returns the sqlite schema for the avro schema
func AvroToSqliteSchema(schema avro.Schema) (*SqliteSchema, error) {

	return nil, errors.New("not implemented")
}

// sqliteTypeToAvroSchema converts a sqlite type to an avro primitve schema.
// Sqlite typoes are convered into the largest avro type that can hold the sqlite type.
// This means that representations are not as dense as they could be, but it is a simple
// way to ensure compatibility.
// https://www.sqlite.org/datatype3.html
// https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive
func sqliteTypeToAvroSchema(t sqliteType) (avro.Schema, error) {
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
	case sqliteBoolean:
		return booleanSchema, nil
	default:
		return nil, fmt.Errorf("unknown sqlite type: %s", t)
	}
}

// ReadAvro reads avro records from an io.Reader into a slice of T
func ReadAvro(schema avro.Schema, r io.Reader) ([]map[string]any, error) {
	out := []map[string]any{}

	decoder, err := avro.NewDecoder(schema.String(), r)
	if err != nil {
		return out, err
	}

	var st map[string]any
	for err == nil {
		err = decoder.Decode(&st)
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, err
		}
		out = append(out, st)
	}

	return out, nil
}
