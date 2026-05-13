// Package avrosqlite provides bidirectional conversion between SQLite databases
// and Apache Avro format (https://avro.apache.org/).
//
// This package reads schemas and data from SQLite databases and converts them
// to Avro format, including support for Object Container Files (OCF). It also
// supports the reverse operation: reading Avro schemas and data and loading
// them into SQLite databases.
//
// # Features
//
// The package provides the following capabilities:
//   - Schema introspection: Automatically detect and convert SQLite table schemas
//   - Data export: Export SQLite tables to Avro OCF files
//   - Data import: Load Avro data into SQLite tables
//   - JSON schema export: Export table schemas as JSON files
//   - Extensibility: Use the [Enhancer] interface to customize schema and data transformation
//
// # Type Mapping
//
// SQLite types are mapped to Avro types as follows:
//   - INTEGER -> long (int64)
//   - REAL -> double (float64)
//   - TEXT -> string
//   - BLOB -> bytes
//   - BOOLEAN -> boolean
//   - NULL -> null
//
// Nullable fields are represented as Avro union types.
//
// # Basic Usage
//
// To export all tables from a SQLite database to Avro files:
//
//	files, err := avrosqlite.SqliteToAvro(db, "./output", "prefix_", true, nil)
//
// To load Avro data into a SQLite table:
//
//	count, err := avrosqlite.LoadAvro(db, schema, reader)
package avrosqlite
