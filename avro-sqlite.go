package avrosqlite

import "errors"

// Read the list of tables from sqlite
// Read the schema of each table
// Generate the avro schema for each table
// Read the data from each table
// Generate the avro data for each table

// SqliteToAvroType converts a sqlite type to an avro type.
// Sqlite typoes are convered into the largest avro type that can hold the sqlite type.
// This means that representations are not as dense as they could be, but it is a simple
// way to ensure compatibility.
// https://www.sqlite.org/datatype3.html
// https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive
func SqliteToAvroType(sqliteType SQLiteType) (AvroType, error) {
	switch sqliteType {
	case SQLiteNull:
		return AvroNull, nil
	case SQLiteInteger:
		return AvroLong, nil
	case SQLiteReal:
		return AvroDouble, nil
	case SQLiteText:
		return AvroString, nil
	case SQLiteBlob:
		return AvroBytes, nil
	default:
		return "", errors.New("unknown sqlite type")
	}
}
