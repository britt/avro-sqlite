// Package avrosqlite provides functionality to interact with SQLite databases
// and convert their schemas and data to Avro format.
package avrosqlite

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/hamba/avro"
)

// SqliteType represents the data type of a SQLite column.
type SqliteType string

// Constants for SQLite data types and default values.
const (
	SqliteNull           SqliteType = "null"
	SqliteInteger        SqliteType = "integer"
	SqliteReal           SqliteType = "real"
	SqliteText           SqliteType = "text"
	SqliteBlob           SqliteType = "blob"
	SqliteBoolean        SqliteType = "boolean"
	SqliteIntegerDefault            = 0
	SqliteRealDefault               = 0.0
	SqliteTextDefault               = ""
)

// SqliteBlobDefault represents the default value for BLOB type.
var SqliteBlobDefault = []byte{}

// sqliteSpecialTables is a list of SQLite system tables to be ignored.
var sqliteSpecialTables = []string{"sqlite_sequence"}

// SqliteSchema represents the schema of a SQLite table.
type SqliteSchema struct {
	Table  string        `json:"table"`
	Fields []SchemaField `json:"fields"`
	Sql    string        `json:"sql"`
}

// SchemaField represents a single field in a SQLite table schema.
type SchemaField struct {
	Name     string     `json:"name"`
	Type     SqliteType `json:"type"`
	Nullable bool       `json:"nullable"`
	Default  any        `json:"default,omitempty"`
}

// AvroDefault returns the default value for a field in the Avro schema.
// TODO: make private
func (s SchemaField) AvroDefault() interface{} {
	if s.Nullable {
		return avro.NoDefault
	}

	switch s.Type {
	case SqliteNull:
		return nil
	case SqliteInteger:
		if _, ok := s.Default.(int64); !ok {
			return SqliteIntegerDefault
		}
	case SqliteReal:
		if _, ok := s.Default.(float64); !ok {
			return SqliteRealDefault
		}
	case SqliteText:
		if _, ok := s.Default.(string); !ok {
			return SqliteTextDefault
		}
	case SqliteBlob:
		if _, ok := s.Default.([]byte); !ok {
			return SqliteBlobDefault
		}
	case SqliteBoolean:
		if b, ok := s.Default.(int); !ok {
			return false
		} else {
			return b != 0
		}
	}
	return s.Default
}

// ToAvro converts the SQLite schema to an Avro schema.
func (s *SqliteSchema) ToAvro() (avro.Schema, error) {
	fields := []*avro.Field{}
	for _, field := range s.Fields {
		s, err := sqliteTypeToAvroSchema(field.Type, field.Nullable)
		if err != nil {
			return nil, fmt.Errorf("failed to convert sqlite type to avro schema: [%w]", err)
		}

		avroField, err := avro.NewField(field.Name, s, field.AvroDefault())
		if err != nil {
			return nil, fmt.Errorf("failed to create avro field: [%w]", err)
		}

		fields = append(fields, avroField)
	}
	record, err := avro.NewRecordSchema(s.Table, "com.github.britt.avrosqlite", fields)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro record schema: [%w]", err)
	}
	return record, nil
}

// ListTables returns a list of user-defined tables in the SQLite database.
// It excludes system tables listed in sqliteSpecialTables.
func ListTables(db *sql.DB) ([]string, error) {
	tables := []string{}
	// Read the list of tables from sqlite
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table';")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return tables, err
		}

		isSpecial := false
		for _, specialTable := range sqliteSpecialTables {
			if tableName == specialTable {
				isSpecial = true
				break
			}
		}
		if isSpecial {
			continue
		}

		tables = append(tables, tableName)
	}
	return tables, nil
}

// tableExists checks if a table with the given name exists in the SQLite database.
func tableExists(db *sql.DB, table string) (bool, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

const sqliteTableInfoQuery = `
SELECT 
    '%s' AS TABLE_SCHEMA,
    "name" AS COLUMN_NAME,
    "type" AS DATA_TYPE,
    CASE when "notnull" = 0 THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,
    "dflt_value" AS COLUMN_DEFAULT
FROM 
    pragma_table_info("%s")
`

const sqliteTableCreationSqlQuery = `
SELECT sql
FROM sqlite_master
WHERE type = 'table' AND name = '%s'
`

// ReadSchema retrieves the schema of a specified SQLite table.
// It returns a SqliteSchema struct containing table name, fields, and creation SQL.
func ReadSchema(db *sql.DB, tableName string) (*SqliteSchema, error) {
	// Read the schema of the table
	rows, err := db.Query(
		fmt.Sprintf(sqliteTableInfoQuery, tableName, tableName),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var createSql string
	sqlRows, err := db.Query(fmt.Sprintf(sqliteTableCreationSqlQuery, tableName))
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()
	if sqlRows.Next() {
		err = sqlRows.Scan(&createSql)
		if err != nil {
			return nil, err
		}
	}

	schema := &SqliteSchema{
		Table:  tableName,
		Fields: []SchemaField{},
		Sql:    createSql,
	}

	var (
		tableSchema        string
		columnName         string
		dataType           string
		isNullableStr      string
		isNullable         bool
		defaultValue       sql.NullString
		defaultSchemaValue any
	)
	for rows.Next() {
		err = rows.Scan(&tableSchema, &columnName, &dataType, &isNullableStr, &defaultValue)
		if err != nil {
			return nil, err
		}
		dataType = strings.ToLower(dataType)
		isNullableStr = strings.ToLower(isNullableStr)
		isNullable = isNullableStr == "yes"
		if defaultValue.Valid {
			// TODO: handle this error?
			defaultSchemaValue, _ = toDefaultValueType(dataType, defaultValue.String)
		} else {
			defaultSchemaValue = avro.NoDefault
		}

		schema.Fields = append(schema.Fields, SchemaField{
			Name:     columnName,
			Type:     SqliteType(dataType),
			Nullable: isNullable,
			Default:  defaultSchemaValue,
		})
	}

	return schema, nil
}

// toDefaultValueType converts a string default value to the appropriate Go type
// based on the SQLite data type.
func toDefaultValueType(dataType string, s string) (any, error) {
	switch SqliteType(dataType) {
	case SqliteNull:
		return nil, nil
	case SqliteInteger:
		return strconv.ParseInt(s, 10, 64)
	case SqliteReal:
		return strconv.ParseFloat(s, 64)
	case SqliteText:
		return s, nil
	case SqliteBlob:
		return []byte(s), nil
	case SqliteBoolean:
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return false, err
		}
		return i != 0, nil
	}
	return nil, fmt.Errorf("unknown sqlite type: %s", dataType)
}

// LoadData retrieves all data from the specified SQLite table.
// It returns a slice of maps, where each map represents a row in the table.
func LoadData(db *sql.DB, table string) ([]map[string]any, error) {
	data := []map[string]any{}
	// Read the data from each table
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return data, err
	}
	defer rows.Close()

	for rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return data, err
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return data, err
		}

		entry := map[string]any{}
		for i, col := range columns {
			val := values[i]
			entry[col] = val
		}
		data = append(data, entry)
	}
	return data, nil
}
