package avrosqlite

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/hamba/avro"
	_ "github.com/mattn/go-sqlite3"
)

var testDB *sql.DB
var err error

func init() {
	testDB, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}

	_, err = testDB.Exec("CREATE TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	if err != nil {
		panic(err)
	}

	_, err = testDB.Exec("INSERT INTO foo (name) VALUES ('bar')")
	if err != nil {
		panic(err)
	}
	_, err = testDB.Exec("INSERT INTO foo (name) VALUES ('bat')")
	if err != nil {
		panic(err)
	}
	_, err = testDB.Exec("INSERT INTO foo (name) VALUES ('baz')")
	if err != nil {
		panic(err)
	}

	_, err = testDB.Exec("CREATE TABLE IF NOT EXISTS meats (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, description TEXT)")
	if err != nil {
		panic(err)
	}

	_, err = testDB.Exec("INSERT INTO meats (name, description) VALUES ('beef', 'a cow')")
	if err != nil {
		panic(err)
	}
	_, err = testDB.Exec("INSERT INTO meats (name, description) VALUES ('pork', 'a pig')")
	if err != nil {
		panic(err)
	}
	_, err = testDB.Exec("INSERT INTO meats (name, description) VALUES ('chicken', 'a bird')")
	if err != nil {
		panic(err)
	}
}

func TestSchemaField_AvroDefault(t *testing.T) {
	type fields struct {
		Name               string
		Type               SqliteType
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
				Type:     SqliteInteger,
				Nullable: true,
				Default:  nil,
			},
			want: avro.NoDefault,
		},
		{
			name: "integer",
			fields: fields{
				Name:     "id",
				Type:     SqliteInteger,
				Nullable: false,
				Default:  int64(4),
			},
			want: int64(4),
		},
		{
			name: "real",
			fields: fields{
				Name:     "id",
				Type:     SqliteReal,
				Nullable: false,
				Default:  0.762,
			},
			want: 0.762,
		},
		{
			name: "text",
			fields: fields{
				Name:     "id",
				Type:     SqliteText,
				Nullable: false,
				Default:  "Luz Noceda",
			},
			want: "Luz Noceda",
		},
		{
			name: "blob",
			fields: fields{
				Name:     "id",
				Type:     SqliteBlob,
				Nullable: false,
				Default:  []byte("Edalyn Clawthorne"),
			},
			want: []byte("Edalyn Clawthorne"),
		},
		{
			name: "boolean",
			fields: fields{
				Name:     "id",
				Type:     SqliteBoolean,
				Nullable: false,
				Default:  4,
			},
			want: true,
		},
		{
			name: "integer bad default",
			fields: fields{
				Name:     "id",
				Type:     SqliteInteger,
				Nullable: false,
				Default:  "meatballs",
			},
			want: SqliteIntegerDefault,
		},
		{
			name: "real bad default",
			fields: fields{
				Name:     "id",
				Type:     SqliteReal,
				Nullable: false,
				Default:  "meatballs",
			},
			want: SqliteRealDefault,
		},
		{
			name: "text bad default",
			fields: fields{
				Name:     "id",
				Type:     SqliteText,
				Nullable: false,
				Default:  42,
			},
			want: SqliteTextDefault,
		},
		{
			name: "blob bad default",
			fields: fields{
				Name:     "id",
				Type:     SqliteBlob,
				Nullable: false,
				Default:  []int{1, 2, 3},
			},
			want: SqliteBlobDefault,
		},
		{
			name: "boolean bad default",
			fields: fields{
				Name:     "id",
				Type:     SqliteBoolean,
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
		want    [32]byte
		wantErr bool
	}{
		{
			name: "happy_path",
			fields: fields{
				Table: "foo",
				Fields: []SchemaField{
					{
						Name:     "id",
						Type:     SqliteInteger,
						Nullable: false,
						Default:  int64(0),
					},
					{
						Name:     "name",
						Type:     SqliteText,
						Nullable: false,
						Default:  "meatballs",
					},
				},
			},
			want:    avro.MustParse(`{"name":"com.github.britt.avrosqlite.foo","type":"record","fields":[{"name":"id","type":"long","default":0},{"name":"name","type":"string","default":"meatballs"}]}`).Fingerprint(),
			wantErr: false,
		},
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
			if !reflect.DeepEqual(got.Fingerprint(), tt.want) {
				t.Errorf("SqliteSchema.ToAvro() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestListTables(t *testing.T) {
	got, err := ListTables(testDB)
	if err != nil {
		t.Errorf("ListTables() error = %v", err)
	}

	if len(got) != 2 {
		t.Errorf("ListTables() = %v, want %v", got, []string{"foo", "meats"})
	}

	for _, table := range got {
		if table != "foo" && table != "meats" {
			t.Errorf("ListTables() = %v, want %v", got, []string{"foo", "meats"})
		}
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
		{
			name: "happy_path",
			args: args{
				db:        testDB,
				tableName: "foo",
			},
			want: &SqliteSchema{
				Table: "foo",
				Fields: []SchemaField{
					{
						Name:     "id",
						Type:     SqliteInteger,
						Nullable: true,
						Default:  avro.NoDefault,
					},
					{
						Name:     "name",
						Type:     SqliteText,
						Nullable: true,
						Default:  avro.NoDefault,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadSchema(tt.args.db, tt.args.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Table != tt.want.Table {
				t.Errorf("ReadSchema() = %v, want %v", got, tt.want)
			}
			if len(got.Fields) != len(tt.want.Fields) {
				t.Errorf("ReadSchema() = %v, want %v", got, tt.want)
			}

			for i, field := range got.Fields {
				if field.Name != tt.want.Fields[i].Name {
					t.Errorf("ReadSchema() Name = %v, want %v", got, tt.want)
				}
				if field.Type != tt.want.Fields[i].Type {
					t.Errorf("ReadSchema() Type = %v, want %v", got, tt.want)
				}
				if field.Nullable != tt.want.Fields[i].Nullable {
					t.Errorf("ReadSchema() Nullable = %v, want %v", got, tt.want)
				}
				if field.Default != avro.NoDefault {
					t.Errorf("ReadSchema() Default = %v, want %v", got, tt.want)
				}
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
		{
			name: "happy_path#foo",
			args: args{
				db:    testDB,
				table: "foo",
			},
			want:    []map[string]any{{"id": int64(1), "name": "bar"}, {"id": int64(2), "name": "bat"}, {"id": int64(3), "name": "baz"}},
			wantErr: false,
		},
		{
			name: "happy_path#meats",
			args: args{
				db:    testDB,
				table: "meats",
			},
			want:    []map[string]any{{"id": int64(1), "name": "beef", "description": "a cow"}, {"id": int64(2), "name": "pork", "description": "a pig"}, {"id": int64(3), "name": "chicken", "description": "a bird"}},
			wantErr: false,
		},
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
