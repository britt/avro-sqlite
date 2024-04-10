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

// TableToOCF writes the data from the table to an OCF file.
func TableToOCF(db *sql.DB, table, fileName string) error {
	schema, err := ReadSchema(db, table)
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

// SqliteToAvro writes the data from the sqlite database to a set of OCF files
// and returns the paths of the files it creates. It is not atomic.
// Errors can result in incomplete sets of files.
func SqliteToAvro(db *sql.DB, path, prefix string) ([]string, error) {
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
		err := TableToOCF(db, table, fileName)
		if err != nil {
			return files, err
		}
		files = append(files, fileName)
	}

	return files, nil
}

// SqliteSchemaToJSON writes the schema of the sqlite database to a set of JSON files
func SqliteSchemaToJSON(db *sql.DB, path, prefix string) ([]string, error) {
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
		fileName := filepath.Join(savePath, fmt.Sprintf("%s%s.json", prefix, table))
		schema, err := ReadSchema(db, table)
		if err != nil {
			return files, err
		}

		b, err := json.Marshal(schema)
		if err != nil {
			return files, err
		}

		f, err := os.Create(fileName)
		if err != nil {
			return files, err
		}
		defer f.Close()

		if _, err = f.Write(b); err != nil {
			return files, err
		}
		if err := f.Sync(); err != nil {
			return files, err
		}

		files = append(files, fileName)
	}

	return files, nil
}
