package avrosqlite

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/hamba/avro"
)

func TestSchemaField_AvroDefault(t *testing.T) {
	type fields struct {
		Name               string
		Type               sqliteType
		Nullable           bool
		Default            any
		NumericPrecision   sql.NullInt64
		NumericScale       sql.NullInt64
		CharacterMaxLength sql.NullInt64
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{
			name: "nullable *",
			fields: fields{
				Name:     "id",
				Type:     sqliteInteger,
				Nullable: true,
				Default:  nil,
			},
			want: avro.NoDefault,
		},
		{
			name: "integer",
			fields: fields{
				Name:     "id",
				Type:     sqliteInteger,
				Nullable: false,
				Default:  int64(4),
			},
			want: int64(4),
		},
		{
			name: "real",
			fields: fields{
				Name:     "id",
				Type:     sqliteReal,
				Nullable: false,
				Default:  0.762,
			},
			want: 0.762,
		},
		{
			name: "text",
			fields: fields{
				Name:     "id",
				Type:     sqliteText,
				Nullable: false,
				Default:  "Luz Noceda",
			},
			want: "Luz Noceda",
		},
		{
			name: "blob",
			fields: fields{
				Name:     "id",
				Type:     sqliteBlob,
				Nullable: false,
				Default:  []byte("Edalyn Clawthorne"),
			},
			want: []byte("Edalyn Clawthorne"),
		},
		{
			name: "boolean",
			fields: fields{
				Name:     "id",
				Type:     sqliteBoolean,
				Nullable: false,
				Default:  4,
			},
			want: true,
		},
		{
			name: "integer bad default",
			fields: fields{
				Name:     "id",
				Type:     sqliteInteger,
				Nullable: false,
				Default:  "meatballs",
			},
			want: sqliteIntegerDefault,
		},
		{
			name: "real bad default",
			fields: fields{
				Name:     "id",
				Type:     sqliteReal,
				Nullable: false,
				Default:  "meatballs",
			},
			want: sqliteRealDefault,
		},
		{
			name: "text bad default",
			fields: fields{
				Name:     "id",
				Type:     sqliteText,
				Nullable: false,
				Default:  42,
			},
			want: sqliteTextDefault,
		},
		{
			name: "blob bad default",
			fields: fields{
				Name:     "id",
				Type:     sqliteBlob,
				Nullable: false,
				Default:  []int{1, 2, 3},
			},
			want: sqliteBlobDefault,
		},
		{
			name: "boolean bad default",
			fields: fields{
				Name:     "id",
				Type:     sqliteBoolean,
				Nullable: false,
				Default:  "meatballs",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SchemaField{
				Name:     tt.fields.Name,
				Type:     tt.fields.Type,
				Nullable: tt.fields.Nullable,
				Default:  tt.fields.Default,
			}
			if got := s.AvroDefault(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SchemaField.AvroDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSqliteSchema_ToAvro(t *testing.T) {
	type fields struct {
		Table  string
		Fields []SchemaField
	}
	tests := []struct {
		name    string
		fields  fields
		want    avro.Schema
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SqliteSchema{
				Table:  tt.fields.Table,
				Fields: tt.fields.Fields,
			}
			got, err := s.ToAvro()
			if (err != nil) != tt.wantErr {
				t.Errorf("SqliteSchema.ToAvro() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SqliteSchema.ToAvro() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestListTables(t *testing.T) {
	type args struct {
		db *sql.DB
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListTables(tt.args.db)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadSchema(t *testing.T) {
	type args struct {
		db        *sql.DB
		tableName string
	}
	tests := []struct {
		name    string
		args    args
		want    *SqliteSchema
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadSchema(tt.args.db, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadData(t *testing.T) {
	type args struct {
		db     *sql.DB
		table  string
		schema avro.Schema
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadData(tt.args.db, tt.args.table, tt.args.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_LoadData(t *testing.T) {
	type args struct {
		db    *sql.DB
		table string
	}
	tests := []struct {
		name    string
		args    args
		want    []map[string]any
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LoadData(tt.args.db, tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadData() = %v, want %v", got, tt.want)
			}
		})
	}
}
