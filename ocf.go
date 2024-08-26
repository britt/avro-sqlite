package avrosqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/hamba/avro/ocf"
)

// Enhancer is an interface for augmenting the schema and the data
// with additional information or computed values.
type Enhancer interface {
	// Schema modifies the schema in place before rows are read.
	// It receives a pointer to the SqliteSchema and can make modifications to it.
	// Returns an error if any issues occur during schema enhancement.
	Schema(*SqliteSchema) error

	// Row modifies each row in place before it is written.
	// It receives a map representing a single row of data and can modify its contents.
	// Returns an error if any issues occur during row enhancement.
	Row(map[string]any) error
}

// TODO: Add a way to modify table creation SQL

type noopEnhancer struct{}

func (*noopEnhancer) Schema(*SqliteSchema) error { return nil }
func (*noopEnhancer) Row(map[string]any) error   { return nil }

// TableToOCF writes the data from a specified table to an OCF (Object Container File) file.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - table: The name of the table to export.
//   - fileName: The path and name of the OCF file to be created.
//   - enhancer: An Enhancer interface for modifying the schema and data (can be nil).
//
// Returns:
//   - error: An error if any occurred during the process, nil otherwise.
//
// This function reads the schema and data from the specified table, applies any enhancements,
// and writes the result to an OCF file.
func TableToOCF(db *sql.DB, table, fileName string, enhancer Enhancer) error {
	if enhancer == nil {
		enhancer = &noopEnhancer{}
	}

	schema, err := ReadSchema(db, table)
	if err != nil {
		return err
	}
	err = enhancer.Schema(schema)
	if err != nil {
		return err
	}

	avroSchema, err := schema.ToAvro()
	if err != nil {
		return err
	}

	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	enc, err := ocf.NewEncoder(avroSchema.String(), f)
	if err != nil {
		log.Fatal(err)
	}
	defer enc.Close()

	data, err := LoadData(db, table)
	if err != nil {
		return err
	}

	for _, row := range data {
		err = enhancer.Row(row)
		if err != nil {
			return err
		}

		err = enc.Encode(row)
		if err != nil {
			return err
		}
	}

	if err := enc.Flush(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}

// TableToJSON writes the schema of a specified table to a JSON file.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - table: The name of the table whose schema is to be exported.
//   - fileName: The path and name of the JSON file to be created.
//   - enhancer: An Enhancer interface for modifying the schema (can be nil).
//
// Returns:
//   - error: An error if any occurred during the process, nil otherwise.
//
// This function reads the schema from the specified table, applies any enhancements,
// and writes the resulting schema to a JSON file.
func TableToJSON(db *sql.DB, table, fileName string, enhancer Enhancer) error {
	if enhancer == nil {
		enhancer = &noopEnhancer{}
	}

	schema, err := ReadSchema(db, table)
	if err != nil {
		return err
	}
	err = enhancer.Schema(schema)
	if err != nil {
		return err
	}

	b, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Write(b); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// SqliteToAvro exports data from a SQLite database to a set of OCF (Object Container File) files.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - path: The directory path where the OCF files will be saved.
//   - prefix: A string to be prepended to each table name in the output file names.
//   - includeJSON: If true, also saves a JSON version of each table's schema.
//   - enhancer: An Enhancer interface for modifying schemas and data (can be nil).
//
// Returns:
//   - []string: A slice of strings containing the paths of all created files.
//   - error: An error if any occurred during the process, nil otherwise.
//
// This function exports all tables from the SQLite database to individual OCF files.
// It optionally includes JSON schema files. The function is not atomic, and errors
// may result in incomplete sets of files.
func SqliteToAvro(db *sql.DB, path, prefix string, includeJSON bool, enhancer Enhancer) ([]string, error) {
	files := []string{}

	tables, err := ListTables(db)
	if err != nil {
		return files, err
	}

	savePath, err := filepath.Abs(path)
	if err != nil {
		return files, err
	}

	for _, table := range tables {
		fileName := filepath.Join(savePath, fmt.Sprintf("%s%s.avro", prefix, table))
		err := TableToOCF(db, table, fileName, enhancer)
		if err != nil {
			return files, err
		}
		files = append(files, fileName)
		if includeJSON {
			jsonFileName := filepath.Join(savePath, fmt.Sprintf("%s%s.json", prefix, table))
			err := TableToJSON(db, table, jsonFileName, enhancer)
			if err != nil {
				return files, err
			}
			files = append(files, jsonFileName)
		}
	}

	return files, nil
}
