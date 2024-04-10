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
	Schema(*SqliteSchema) error
	// Row modifies each row in place before it is written.
	Row(map[string]any) error
}

type noopEnhancer struct{}

func (*noopEnhancer) Schema(*SqliteSchema) error { return nil }
func (*noopEnhancer) Row(map[string]any) error   { return nil }

// TableToOCF writes the data from the table to an OCF file.
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

// TableToJSON writes the schema of the table to a JSON file.
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

// SqliteToAvro writes the data from the sqlite database to a set of OCF files
// and returns the paths of the files it creates. It can optionally include a JSON version
// of the schema in the same directory. The prefix is added to the front of the table name.
// It is not atomic. Errors can result in incomplete sets of files.
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
