package avrosqlite

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

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

// LoadAvro loads Avro data into a SQLite database.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - schema: A pointer to the SqliteSchema containing the table structure.
//   - r: An io.Reader providing the Avro data to be loaded.
//
// Returns:
//   - int64: The number of records successfully inserted into the database.
//   - error: An error if any occurred during the process, nil otherwise.
//
// If the specified table does not exist in the database, it will be created.
// If the table already exists, it will be truncated before inserting new data.
func LoadAvro(db *sql.DB, schema *SqliteSchema, r io.Reader) (int64, error) {
	avroSchema, err := schema.ToAvro()
	if err != nil {
		return 0, err
	}
	decoder, err := avro.NewDecoder(avroSchema.String(), r)
	if err != nil {
		return 0, err
	}

	// detect if the table exists
	exists, err := tableExists(db, schema.Table)
	if err != nil {
		return 0, err
	}
	// create a table in the database
	if !exists {
		_, err := db.Exec(schema.Sql)
		if err != nil {
			return 0, err
		}
	} else {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s", schema.Table))
		if err != nil {
			return 0, err
		}
	}
	// generate an insert prepared statement
	fieldNames := []string{}
	for _, f := range schema.Fields {
		fieldNames = append(fieldNames, f.Name)
	}
	insertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", schema.Table, strings.Join(fieldNames, ", "), strings.Repeat("?, ", len(schema.Fields)-1)+"?")
	stmt, err := db.Prepare(insertSql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// for each record in the avro file
	var count int64
	var st map[string]any
	for err == nil {
		err = decoder.Decode(&st)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		args := []any{}
		for _, f := range fieldNames {
			args = append(args, st[f])
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			return count, err
		}
		count += 1
	}
	// insert the record into the database
	return count, nil
}

// sqliteTypeToAvroSchema converts a sqlite type to an avro primitve schema.
// Sqlite typoes are convered into the largest avro type that can hold the sqlite type.
// This means that representations are not as dense as they could be, but it is a simple
// way to ensure compatibility.
// https://www.sqlite.org/datatype3.html
// https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive
func sqliteTypeToAvroSchema(t SqliteType, nullable bool) (avro.Schema, error) {
	var avroSchema avro.Schema
	switch t {
	case SqliteNull:
		avroSchema = nullSchema
	case SqliteInteger:
		avroSchema = longSchema
	case SqliteReal:
		avroSchema = doubleSchema
	case SqliteText:
		avroSchema = stringSchema
	case SqliteBlob:
		avroSchema = bytesSchema
	case SqliteBoolean:
		avroSchema = booleanSchema
	default:
		return nil, fmt.Errorf("unknown sqlite type: %s", t)
	}

	if nullable {
		return avro.NewUnionSchema([]avro.Schema{nullSchema, avroSchema})
	}

	return avroSchema, nil
}

// ReadAvro reads Avro records from an io.Reader and returns them as a slice of maps.
//
// Parameters:
//   - schema: The Avro schema used to decode the data.
//   - r: An io.Reader providing the Avro data to be read.
//
// Returns:
//   - []map[string]any: A slice of maps, where each map represents an Avro record.
//     The keys are field names, and the values are the corresponding field values.
//   - error: An error if any occurred during the reading process, nil otherwise.
//
// This function decodes Avro records until it reaches the end of the input or encounters an error.
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
package avrosqlite

import (
	"database/sql"
	"fmt"
	"io"
	"strings"

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

// LoadAvro loads Avro data into a SQLite database.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - schema: A pointer to the SqliteSchema containing the table structure.
//   - r: An io.Reader providing the Avro data to be loaded.
//
// Returns:
//   - int64: The number of records successfully inserted into the database.
//   - error: An error if any occurred during the process, nil otherwise.
//
// If the specified table does not exist in the database, it will be created.
// If the table already exists, it will be truncated before inserting new data.
func LoadAvro(db *sql.DB, schema *SqliteSchema, r io.Reader) (int64, error) {
	avroSchema, err := schema.ToAvro()
	if err != nil {
		return 0, err
	}
	decoder, err := avro.NewDecoder(avroSchema.String(), r)
	if err != nil {
		return 0, err
	}

	// detect if the table exists
	exists, err := tableExists(db, schema.Table)
	if err != nil {
		return 0, err
	}
	// create a table in the database
	if !exists {
		_, err := db.Exec(schema.Sql)
		if err != nil {
			return 0, err
		}
	} else {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s", schema.Table))
		if err != nil {
			return 0, err
		}
	}
	// generate an insert prepared statement
	fieldNames := []string{}
	for _, f := range schema.Fields {
		fieldNames = append(fieldNames, f.Name)
	}
	insertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", schema.Table, strings.Join(fieldNames, ", "), strings.Repeat("?, ", len(schema.Fields)-1)+"?")
	stmt, err := db.Prepare(insertSql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// for each record in the avro file
	var count int64
	var st map[string]any
	for err == nil {
		err = decoder.Decode(&st)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		args := []any{}
		for _, f := range fieldNames {
			args = append(args, st[f])
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			return count, err
		}
		count += 1
	}
	// insert the record into the database
	return count, nil
}

// sqliteTypeToAvroSchema converts a sqlite type to an avro primitve schema.
// Sqlite typoes are convered into the largest avro type that can hold the sqlite type.
// This means that representations are not as dense as they could be, but it is a simple
// way to ensure compatibility.
// https://www.sqlite.org/datatype3.html
// https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive
func sqliteTypeToAvroSchema(t SqliteType, nullable bool) (avro.Schema, error) {
	var avroSchema avro.Schema
	switch t {
	case SqliteNull:
		avroSchema = nullSchema
	case SqliteInteger:
		avroSchema = longSchema
	case SqliteReal:
		avroSchema = doubleSchema
	case SqliteText:
		avroSchema = stringSchema
	case SqliteBlob:
		avroSchema = bytesSchema
	case SqliteBoolean:
		avroSchema = booleanSchema
	default:
		return nil, fmt.Errorf("unknown sqlite type: %s", t)
	}

	if nullable {
		return avro.NewUnionSchema([]avro.Schema{nullSchema, avroSchema})
	}

	return avroSchema, nil
}

// ReadAvro reads Avro records from an io.Reader and returns them as a slice of maps.
//
// Parameters:
//   - schema: The Avro schema used to decode the data.
//   - r: An io.Reader providing the Avro data to be read.
//
// Returns:
//   - []map[string]any: A slice of maps, where each map represents an Avro record.
//     The keys are field names, and the values are the corresponding field values.
//   - error: An error if any occurred during the reading process, nil otherwise.
//
// This function decodes Avro records until it reaches the end of the input or encounters an error.
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
