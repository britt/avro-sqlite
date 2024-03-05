package avrosqlite

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hamba/avro"
)

type SQLiteType string

const (
	SQLiteNull           SQLiteType = "null"
	SQLiteInteger        SQLiteType = "integer"
	SQLiteReal           SQLiteType = "real"
	SQLiteText           SQLiteType = "text"
	SQLiteBlob           SQLiteType = "blob"
	SQLiteIntegerDefault            = 0
	SQLiteRealDefault               = 0.0
	SQLiteTextDefault               = ""
)

var SQLiteBlobDefault = []byte{}

type SqliteSchema struct {
	Table  string        `json:"table"`
	Fields []SchemaField `json:"fields"`
}

type SchemaField struct {
	Name               string        `json:"name"`
	Type               SQLiteType    `json:"type"`
	Nullable           bool          `json:"nullable"`
	Default            interface{}   `json:"default,omitempty"`
	NumericPrecision   sql.NullInt64 `json:"numeric_precision,omitempty"`
	NumericScale       sql.NullInt64 `json:"numeric_scale,omitempty"`
	CharacterMaxLength sql.NullInt64 `json:"character_max_length,omitempty"`
}

func (s SchemaField) AvroDefault() interface{} {
	if s.Nullable || s.Type == SQLiteNull {
		return nil
	}

	switch s.Type {
	case SQLiteInteger:
		if s.Default == nil {
			return SQLiteIntegerDefault
		}
	case SQLiteReal:
		if s.Default == nil {
			return SQLiteRealDefault
		}
	case SQLiteText:
		if s.Default == nil {
			return SQLiteTextDefault
		}
	case SQLiteBlob:
		if s.Default == nil {
			return SQLiteBlobDefault
		}
	}
	return s.Default
}

// ToAvro returns the avro schema for the sqlite schema
func (s *SqliteSchema) ToAvro() (avro.Schema, error) {
	fields := []*avro.Field{}
	for _, field := range s.Fields {
		s, err := SqliteToAvroSchema(field.Type)
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

// ListTables returns a list of tables in the sqlite database
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
		tables = append(tables, tableName)
	}
	return tables, nil
}

const SQLITE_TABLE_INFO_QUERY = `
SELECT 
    '%s' AS TABLE_SCHEMA,
    "name" AS COLUMN_NAME,
    "type" AS DATA_TYPE,
    CASE when "notnull" = 0 THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,
    "dflt_value" AS COLUMN_DEFAULT,
    null AS NUMERIC_PRECISION,
    null AS NUMERIC_SCALE,
    CASE when "type" = 'TEXT' THEN (SELECT MAX(LENGTH(name)) FROM %s) END AS CHARACTER_MAXIMUM_LENGTH
FROM 
    pragma_table_info("%s")
`

// ReadSchema returns the schema of the sqlite table
func ReadSchema(db *sql.DB, tableName string) (*SqliteSchema, error) {
	// Read the schema of the table
	rows, err := db.Query(
		fmt.Sprintf(SQLITE_TABLE_INFO_QUERY, tableName, tableName, tableName),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := &SqliteSchema{
		Table:  tableName,
		Fields: []SchemaField{},
	}

	var (
		tableSchema       string
		columnName        string
		dataType          string
		isNullableStr     string
		isNullable        bool
		defaultValue      sql.NullString
		defaultValueBytes []byte
		numPrecision      sql.NullInt64
		numScale          sql.NullInt64
		charBytesLen      sql.NullInt64
	)
	for rows.Next() {
		err = rows.Scan(&tableSchema, &columnName, &dataType, &isNullableStr, &defaultValue, &numPrecision, &numScale, &charBytesLen)
		if err != nil {
			return nil, err
		}
		dataType = strings.ToLower(dataType)
		isNullableStr = strings.ToLower(isNullableStr)
		isNullable = isNullableStr == "yes"
		if defaultValue.Valid {
			defaultValueBytes = []byte(defaultValue.String)
		} else {
			defaultValueBytes = nil
		}

		schema.Fields = append(schema.Fields, SchemaField{
			Name:               columnName,
			Type:               SQLiteType(dataType),
			Nullable:           isNullable,
			Default:            string(defaultValueBytes), // TODO: parse appropriate type
			NumericPrecision:   numPrecision,
			NumericScale:       numScale,
			CharacterMaxLength: charBytesLen,
		})
	}

	return schema, nil
}

// ReadData returns the data from the sqlite database
func ReadData(db *sql.DB, table string) ([]map[string]interface{}, error) {
	data := []map[string]interface{}{}
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

		entry := map[string]interface{}{}
		for i, col := range columns {
			val := values[i]
			entry[col] = val
		}
		data = append(data, entry)
	}
	return data, nil
}
