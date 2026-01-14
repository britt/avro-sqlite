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

// SQLite data type constants representing the storage classes defined in SQLite.
// See https://www.sqlite.org/datatype3.html for more information.
const (
	SqliteNull    SqliteType = "null"    // SqliteNull represents the NULL storage class.
	SqliteInteger SqliteType = "integer" // SqliteInteger represents the INTEGER storage class (64-bit signed integer).
	SqliteReal    SqliteType = "real"    // SqliteReal represents the REAL storage class (64-bit IEEE floating point).
	SqliteText    SqliteType = "text"    // SqliteText represents the TEXT storage class (UTF-8, UTF-16BE or UTF-16LE string).
	SqliteBlob    SqliteType = "blob"    // SqliteBlob represents the BLOB storage class (binary large object).
	SqliteBoolean SqliteType = "boolean" // SqliteBoolean represents boolean values (stored as INTEGER 0 or 1).
)

// Default values for SQLite types when no default is specified in the schema.
// These are used during Avro schema generation for non-nullable fields.
const (
	SqliteIntegerDefault = 0   // SqliteIntegerDefault is the default value for INTEGER columns.
	SqliteRealDefault    = 0.0 // SqliteRealDefault is the default value for REAL columns.
	SqliteTextDefault    = ""  // SqliteTextDefault is the default value for TEXT columns.
)

// SqliteBlobDefault is the default value for BLOB columns (empty byte slice).
// This is a variable rather than a constant because []byte cannot be a constant in Go.
var SqliteBlobDefault = []byte{}

// sqliteSpecialTables contains SQLite system tables that should be excluded
// from table listings. These tables are automatically created by SQLite for
// internal bookkeeping and are not user data.
var sqliteSpecialTables = []string{"sqlite_sequence"}

// SqliteSchema represents the complete schema of a SQLite table, including
// its name, column definitions, and the original SQL CREATE statement.
// This struct can be serialized to JSON and is used for both schema export
// and for converting to Avro schemas.
type SqliteSchema struct {
	Table  string        `json:"table"`  // Table is the name of the SQLite table.
	Fields []SchemaField `json:"fields"` // Fields contains the column definitions for the table.
	Sql    string        `json:"sql"`    // Sql is the original CREATE TABLE statement.
}

// SchemaField represents a single column in a SQLite table schema.
// It contains the column's metadata including name, type, nullability,
// and default value if any.
type SchemaField struct {
	Name     string     `json:"name"`             // Name is the column name.
	Type     SqliteType `json:"type"`             // Type is the SQLite data type of the column.
	Nullable bool       `json:"nullable"`         // Nullable indicates whether the column allows NULL values.
	Default  any        `json:"default,omitempty"` // Default is the default value for the column, if specified.
}

// AvroDefault returns the appropriate default value for this field when
// generating an Avro schema.
//
// For nullable fields, it returns [avro.NoDefault] since Avro union types
// with null as the first type don't require explicit defaults.
//
// For non-nullable fields, it returns:
//   - The field's default value if one is set and has the correct type
//   - A type-appropriate zero value (0 for integer, 0.0 for real, "" for text,
//     empty []byte for blob, false for boolean) if no default is set
//   - nil for null type fields
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

// ToAvro converts this SQLite table schema to an equivalent Avro record schema.
//
// The resulting Avro schema will have the table name as its record name and
// use "com.github.britt.avrosqlite" as the namespace. Each SQLite column
// becomes an Avro field with appropriate type mapping (see [sqliteTypeToAvroSchema]).
//
// Nullable columns are represented as Avro union types with null as an option.
// Default values are preserved where applicable.
//
// Returns an error if any field type cannot be converted or if the Avro schema
// construction fails.
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

// ListTables returns a list of user-defined table names in the SQLite database.
//
// It queries sqlite_master for all tables and filters out SQLite system tables
// (such as sqlite_sequence) that are used for internal bookkeeping.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//
// Returns:
//   - []string: A slice of table names.
//   - error: An error if the database query fails.
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
// It queries the sqlite_master table to determine table existence.
// Returns true if the table exists, false otherwise. Returns an error if the
// database query fails.
func tableExists(db *sql.DB, table string) (bool, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

// sqliteTableInfoQuery is a SQL query template that retrieves column information
// for a SQLite table using the pragma_table_info function. The first %s is
// used as the table schema identifier, and the second %s is the actual table name.
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

// sqliteTableCreationSqlQuery is a SQL query template that retrieves the
// original CREATE TABLE statement for a given table from sqlite_master.
// The %s placeholder should be replaced with the table name.
const sqliteTableCreationSqlQuery = `
SELECT sql
FROM sqlite_master
WHERE type = 'table' AND name = '%s'
`

// ReadSchema retrieves the complete schema of a specified SQLite table.
//
// It queries the database to obtain column information (name, type, nullability,
// default values) and the original CREATE TABLE SQL statement.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - tableName: The name of the table whose schema should be retrieved.
//
// Returns:
//   - *SqliteSchema: The table's schema including all column definitions.
//   - error: An error if the table doesn't exist or the query fails.
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

// toDefaultValueType converts a string representation of a default value to the
// appropriate Go type based on the SQLite data type.
//
// SQLite stores default values as strings in the schema metadata, so this function
// parses them into the correct Go types:
//   - null -> nil
//   - integer -> int64 (parsed from string)
//   - real -> float64 (parsed from string)
//   - text -> string (unchanged)
//   - blob -> []byte (converted from string)
//   - boolean -> bool (parsed as integer, non-zero = true)
//
// Returns an error if parsing fails or if the data type is unknown.
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

// LoadData retrieves all rows from the specified SQLite table.
//
// Each row is returned as a map where keys are column names and values are
// the corresponding cell values. The values retain their SQLite types as
// represented by the database/sql driver.
//
// Parameters:
//   - db: A pointer to the sql.DB representing the SQLite database connection.
//   - table: The name of the table to read data from.
//
// Returns:
//   - []map[string]any: A slice of maps, one per row in the table.
//   - error: An error if the query fails or if scanning rows fails.
//
// Note: This function loads all data into memory at once. For very large tables,
// consider using pagination or streaming approaches.
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
