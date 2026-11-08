package mocks

import (
	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/importers"
	"github.com/aarondl/strmangle"
)

func init() {
	drivers.RegisterFromInit("mock", &MockDriver{})
}

// MockDriver is a mock implementation of the bdb driver Interface
type MockDriver struct{}

// Templates returns the overriding templates for the driver
func (m *MockDriver) Templates() (map[string]string, error) {
	return nil, nil
}

// Imports return the set of imports that should be merged
func (m *MockDriver) Imports() (importers.Collection, error) {
	return importers.Collection{
		BasedOnType: importers.Map{
			"null.Int": {
				ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
			},
			"null.String": {
				ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
			},
			"null.Time": {
				ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
			},
			"null.Bytes": {
				ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
			},

			"time.Time": {
				Standard: importers.List{`"time"`},
			},
		},
	}, nil
}

// Assemble the DBInfo
func (m *MockDriver) Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	dbinfo = &drivers.DBInfo{
		Dialect: drivers.Dialect{
			LQ: '"',
			RQ: '"',

			UseIndexPlaceholders: true,
			UseLastInsertID:      false,
			UseTopClause:         false,
		},
	}

	defer func() {
		if r := recover(); r != nil && err == nil {
			dbinfo = nil
			err = r.(error)
		}
	}()

	schema := config.MustString(drivers.ConfigSchema)
	whitelist, _ := config.StringSlice(drivers.ConfigWhitelist)
	blacklist, _ := config.StringSlice(drivers.ConfigBlacklist)

	dbinfo.Tables, err = drivers.TablesConcurrently(m, schema, whitelist, blacklist, 1)
	if err != nil {
		return nil, err
	}

	return dbinfo, err
}

// TableNames returns a list of mock table names
func (m *MockDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	if len(whitelist) > 0 {
		return whitelist, nil
	}
	tables := []string{"pilots", "jets", "airports", "licenses", "hangars", "languages", "pilot_languages"}
	return strmangle.SetComplement(tables, blacklist), nil
}

// Columns returns a list of mock columns
func (m *MockDriver) Columns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	return map[string][]drivers.Column{
		"pilots": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "name", Type: "string", DBType: "character"},
		},
		"airports": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "size", Type: "null.Int", DBType: "integer", Nullable: true},
		},
		"jets": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "pilot_id", Type: "int", DBType: "integer", Nullable: true, Unique: true},
			{Name: "airport_id", Type: "int", DBType: "integer"},
			{Name: "name", Type: "string", DBType: "character", Nullable: false},
			{Name: "color", Type: "null.String", DBType: "character", Nullable: true},
			{Name: "uuid", Type: "string", DBType: "uuid", Nullable: true},
			{Name: "identifier", Type: "string", DBType: "uuid", Nullable: false},
			{Name: "cargo", Type: "[]byte", DBType: "bytea", Nullable: false},
			{Name: "manifest", Type: "[]byte", DBType: "bytea", Nullable: true, Unique: true},
		},
		"licenses": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "pilot_id", Type: "int", DBType: "integer"},
		},
		"hangars": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "name", Type: "string", DBType: "character", Nullable: true, Unique: true},
		},
		"languages": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "language", Type: "string", DBType: "character", Nullable: false, Unique: true},
		},
		"pilot_languages": {
			{Name: "pilot_id", Type: "int", DBType: "integer"},
			{Name: "language_id", Type: "int", DBType: "integer"},
		},
		"personas": {
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "name", Type: "string", DBType: "character"},
		},
	}[tableName], nil
}

// ForeignKeyInfo returns a list of mock foreignkeys
func (m *MockDriver) ForeignKeyInfo(schema, tableName string) ([]drivers.ForeignKey, error) {
	return map[string][]drivers.ForeignKey{
		"jets": {
			{Table: "jets", Name: "jets_pilot_id_fk", Column: "pilot_id", ForeignTable: "pilots", ForeignColumn: "id", ForeignColumnUnique: true},
			{Table: "jets", Name: "jets_airport_id_fk", Column: "airport_id", ForeignTable: "airports", ForeignColumn: "id"},
		},
		"licenses": {
			{Table: "licenses", Name: "licenses_pilot_id_fk", Column: "pilot_id", ForeignTable: "pilots", ForeignColumn: "id"},
		},
		"pilot_languages": {
			{Table: "pilot_languages", Name: "pilot_id_fk", Column: "pilot_id", ForeignTable: "pilots", ForeignColumn: "id"},
			{Table: "pilot_languages", Name: "jet_id_fk", Column: "language_id", ForeignTable: "languages", ForeignColumn: "id"},
		},
	}[tableName], nil
}

// TranslateColumnType converts a column to its "null." form if it is nullable
func (m *MockDriver) TranslateColumnType(c drivers.Column) drivers.Column {
	if c.Nullable {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "null.Int64"
		case "integer", "serial":
			c.Type = "null.Int"
		case "smallint", "smallserial":
			c.Type = "null.Int16"
		case "decimal", "numeric", "double precision":
			c.Type = "null.Float64"
		case `"char"`:
			c.Type = "null.Byte"
		case "bytea":
			c.Type = "null.Bytes"
		case "boolean":
			c.Type = "null.Bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone":
			c.Type = "null.Time"
		default:
			c.Type = "null.String"
		}
	} else {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "int64"
		case "integer", "serial":
			c.Type = "int"
		case "smallint", "smallserial":
			c.Type = "int16"
		case "decimal", "numeric", "double precision":
			c.Type = "float64"
		case `"char"`:
			c.Type = "types.Byte"
		case "bytea":
			c.Type = "[]byte"
		case "boolean":
			c.Type = "bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone":
			c.Type = "time.Time"
		default:
			c.Type = "string"
		}
	}

	return c
}

// PrimaryKeyInfo returns mock primary key info for the passed in table name
func (m *MockDriver) PrimaryKeyInfo(schema, tableName string) (*drivers.PrimaryKey, error) {
	return map[string]*drivers.PrimaryKey{
		"pilots": {
			Name:    "pilot_id_pkey",
			Columns: []string{"id"},
		},
		"airports": {
			Name:    "airport_id_pkey",
			Columns: []string{"id"},
		},
		"jets": {
			Name:    "jet_id_pkey",
			Columns: []string{"id"},
		},
		"licenses": {
			Name:    "license_id_pkey",
			Columns: []string{"id"},
		},
		"hangars": {
			Name:    "hangar_id_pkey",
			Columns: []string{"id"},
		},
		"languages": {
			Name:    "language_id_pkey",
			Columns: []string{"id"},
		},
		"pilot_languages": {
			Name:    "pilot_languages_pkey",
			Columns: []string{"pilot_id", "language_id"},
		},
		"personas": {
			Name:    "personas_pkey",
			Columns: []string{"id"},
		},
	}[tableName], nil
}

// UseLastInsertID returns a database mock LastInsertID compatibility flag
func (m *MockDriver) UseLastInsertID() bool { return false }

// UseTopClause returns a database mock SQL TOP clause compatibility flag
func (m *MockDriver) UseTopClause() bool { return false }

// Open mimics a database open call and returns nil for no error
func (m *MockDriver) Open() error { return nil }

// Close mimics a database close call
func (m *MockDriver) Close() {}

// RightQuote is the quoting character for the right side of the identifier
func (m *MockDriver) RightQuote() byte {
	return '"'
}

// LeftQuote is the quoting character for the left side of the identifier
func (m *MockDriver) LeftQuote() byte {
	return '"'
}

// UseIndexPlaceholders returns true to indicate fake support of indexed placeholders
func (m *MockDriver) UseIndexPlaceholders() bool {
	return false
}
