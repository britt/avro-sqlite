package avrosqlite

import "errors"

type AvroType string

const (
	AvroNull   AvroType = "null"
	AvroLong   AvroType = "long"
	AvroDouble AvroType = "double"
	AvroString AvroType = "string"
	AvroBytes  AvroType = "bytes"
)

type AvroSchema struct{}

// ToSqlite returns the sqlite schema for the avro schema
func (s *AvroSchema) ToSqlite() (*SqliteSchema, error) {
	return nil, errors.New("not implemented")
}

// ToBytes returns the serialized avro schema
func (s *AvroSchema) ToBytes() ([]byte, error) {
	return nil, errors.New("not implemented")
}

// FromBytes returns the avro schema from the serialized avro schema
func FromBytes(b []byte) (*AvroSchema, error) {
	return nil, errors.New("not implemented")
}

// ToAvroData returns the avro data for the sqlite data
func ToAvroData(data []map[string]interface{}) ([]byte, error) {
	return nil, errors.New("not implemented")
}
