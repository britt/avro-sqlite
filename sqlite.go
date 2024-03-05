package avrosqlite

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hamba/avro"
)

type sqliteType string

const (
	sqliteNull           sqliteType = "null"
	sqliteInteger        sqliteType = "integer"
	sqliteReal           sqliteType = "real"
	sqliteText           sqliteType = "text"
	sqliteBlob           sqliteType = "blob"
	sqliteIntegerDefault            = 0
	sqliteRealDefault               = 0.0
	sqliteTextDefault               = ""
)

var sqliteBlobDefault = []byte{}

type SqliteSchema struct {
	Table  string        `json:"table"`
	Fields []SchemaField `json:"fields"`
}

type SchemaField struct {
	Name               string        `json:"name"`
	Type               sqliteType    `json:"type"`
	Nullable           bool          `json:"nullable"`
	Default            any           `json:"default,omitempty"`
	NumericPrecision   sql.NullInt64 `json:"numeric_precision,omitempty"`
	NumericScale       sql.NullInt64 `json:"numeric_scale,omitempty"`
	CharacterMaxLength sql.NullInt64 `json:"character_max_length,omitempty"`
}

func (s SchemaField) AvroDefault() any {
	if s.Type == sqliteNull {
		return nil
	}

	switch s.Type {
	case sqliteInteger:
		if s.Default == nil {
			return sqliteIntegerDefault
		}
	case sqliteReal:
		if s.Default == nil {
			return sqliteRealDefault
		}
	case sqliteText:
		if s.Default == nil {
			return sqliteTextDefault
		}
	case sqliteBlob:
		if s.Default == nil {
			return sqliteBlobDefault
		}
	}
	return s.Default
}

// ToAvro returns the avro schema for the sqlite schema
func (s *SqliteSchema) ToAvro() (avro.Schema, error) {
	fields := []*avro.Field{}
	for _, field := range s.Fields {
		s, err := sqliteToAvroSchema(field.Type)
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

const sqliteTableInfoQuery = `
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
		fmt.Sprintf(sqliteTableInfoQuery, tableName, tableName, tableName),
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
			Type:               sqliteType(dataType),
			Nullable:           isNullable,
			Default:            defaultValueBytes, // TODO: parse appropriate type
			NumericPrecision:   numPrecision,
			NumericScale:       numScale,
			CharacterMaxLength: charBytesLen,
		})
	}

	return schema, nil
}

// ReadData reads the data from the sqlite database table and returns an AVRO encoded byte array
func ReadData(db *sql.DB, table string, schema avro.Schema) ([]byte, error) {
	data, err := loadData(db, table)
	if err != nil {
		return nil, err
	}

	w := bytes.NewBuffer([]byte{})
	e := avro.NewEncoderForSchema(schema, w)
	for _, d := range data {
		err = e.Encode(d)
		if err != nil {
			return nil, err
		}
	}
	return w.Bytes(), nil
}

func loadData(db *sql.DB, table string) ([]map[string]any, error) {
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
