package avrosqlite

import (
	"database/sql"
	"fmt"
	"strconv"
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
	sqliteBoolean        sqliteType = "boolean"
	sqliteIntegerDefault            = 0
	sqliteRealDefault               = 0.0
	sqliteTextDefault               = ""
)

var sqliteBlobDefault = []byte{}

var sqliteSpecialTables = []string{"sqlite_sequence"}

type SqliteSchema struct {
	Table  string        `json:"table"`
	Fields []SchemaField `json:"fields"`
	Sql    string        `json:"sql"`
}

type SchemaField struct {
	Name     string     `json:"name"`
	Type     sqliteType `json:"type"`
	Nullable bool       `json:"nullable"`
	Default  any        `json:"default,omitempty"`
}

func (s SchemaField) AvroDefault() interface{} {
	if s.Nullable {
		return avro.NoDefault
	}

	switch s.Type {
	case sqliteNull:
		return nil
	case sqliteInteger:
		if _, ok := s.Default.(int64); !ok {
			return sqliteIntegerDefault
		}
	case sqliteReal:
		if _, ok := s.Default.(float64); !ok {
			return sqliteRealDefault
		}
	case sqliteText:
		if _, ok := s.Default.(string); !ok {
			return sqliteTextDefault
		}
	case sqliteBlob:
		if _, ok := s.Default.([]byte); !ok {
			return sqliteBlobDefault
		}
	case sqliteBoolean:
		if b, ok := s.Default.(int); !ok {
			return false
		} else {
			return b != 0
		}
	}
	return s.Default
}

// ToAvro returns the avro schema for the sqlite schema
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

// ReadSchema returns the schema of the sqlite table
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
			Type:     sqliteType(dataType),
			Nullable: isNullable,
			Default:  defaultSchemaValue,
		})
	}

	return schema, nil
}

func toDefaultValueType(dataType string, s string) (any, error) {
	switch sqliteType(dataType) {
	case sqliteNull:
		return nil, nil
	case sqliteInteger:
		return strconv.ParseInt(s, 10, 64)
	case sqliteReal:
		return strconv.ParseFloat(s, 64)
	case sqliteText:
		return s, nil
	case sqliteBlob:
		return []byte(s), nil
	case sqliteBoolean:
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return false, err
		}
		return i != 0, nil
	}
	return nil, fmt.Errorf("unknown sqlite type: %s", dataType)
}

// LoadData loads the data from the sqlite database table
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
