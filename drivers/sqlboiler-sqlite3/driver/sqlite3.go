// Package driver is an sqlite driver.
package driver

import (
	"database/sql"
	"embed"
	"encoding/base64"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/importers"
)

//go:embed override
var templates embed.FS

func init() {
	drivers.RegisterFromInit("sqlite3", &SQLiteDriver{})
}

// Assemble the db info
func Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	driver := &SQLiteDriver{}
	return driver.Assemble(config)
}

// SQLiteDriver holds the database connection string and a handle
// to the database connection.
type SQLiteDriver struct {
	connStr           string
	dbConn            *sql.DB
	configForeignKeys []drivers.ForeignKey
}

// Templates that should be added/overridden
func (s SQLiteDriver) Templates() (map[string]string, error) {
	tpls := make(map[string]string)
	fs.WalkDir(templates, "override", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		b, err := fs.ReadFile(templates, path)
		if err != nil {
			return err
		}
		tpls[strings.Replace(path, "override/", "", 1)] = base64.StdEncoding.EncodeToString(b)

		return nil
	})

	return tpls, nil
}

// Assemble the db info
func (s SQLiteDriver) Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			dbinfo = nil
			err = r.(error)
		}
	}()

	dbname := config.MustString(drivers.ConfigDBName)
	whitelist, _ := config.StringSlice(drivers.ConfigWhitelist)
	blacklist, _ := config.StringSlice(drivers.ConfigBlacklist)
	concurrency := config.DefaultInt(drivers.ConfigConcurrency, drivers.DefaultConcurrency)

	if !filepath.IsAbs(dbname) {
		dbname, err = filepath.Abs(dbname)
		if err != nil {
			return nil, fmt.Errorf("sqlboiler-sqlite failed to resolve database path: %w", err)
		}
	}

	s.connStr = SQLiteBuildQueryString(dbname)
	s.configForeignKeys = config.MustForeignKeys(drivers.ConfigForeignKeys)
	s.dbConn, err = sql.Open("sqlite", s.connStr)
	if err != nil {
		return nil, fmt.Errorf("sqlboiler-sqlite failed to connect to database: %w", err)
	}

	defer func() {
		if e := s.dbConn.Close(); e != nil {
			dbinfo = nil
			err = e
		}
	}()

	dbinfo = &drivers.DBInfo{
		Dialect: drivers.Dialect{
			LQ: '"',
			RQ: '"',

			UseSchema:         false,
			UseDefaultKeyword: true,
			UseLastInsertID:   false,
		},
	}

	dbinfo.Tables, err = drivers.TablesConcurrently(s, "", whitelist, blacklist, concurrency)
	if err != nil {
		return nil, err
	}

	return dbinfo, err
}

// SQLiteBuildQueryString builds a query string for SQLite.
func SQLiteBuildQueryString(file string) string {
	return "file:" + file + "?_loc=UTC&mode=ro"
}

// Open opens the database connection using the connection string
func (s SQLiteDriver) Open() error {
	var err error

	s.dbConn, err = sql.Open("sqlite3", s.connStr)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the database connection
func (s SQLiteDriver) Close() {
	s.dbConn.Close()
}

// TableNames connects to the sqlite database and
// retrieves all table names from sqlite_master
func (s SQLiteDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	query := `SELECT name FROM sqlite_master WHERE type='table'`
	args := []any{}

	if len(whitelist) > 0 {
		tables := drivers.TablesFromList(whitelist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and tbl_name in (%s)", strings.Repeat(",?", len(tables))[1:])
			for _, w := range tables {
				args = append(args, w)
			}
		}
	}

	if len(blacklist) > 0 {
		tables := drivers.TablesFromList(blacklist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and tbl_name not in (%s)", strings.Repeat(",?", len(tables))[1:])
			for _, b := range tables {
				args = append(args, b)
			}
		}
	}

	rows, err := s.dbConn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	var names []string
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "sqlite_sequence" {
			names = append(names, name)
		}
	}

	return names, nil
}

// ViewNames connects to the sqlite database and
// retrieves all view names from sqlite_master
func (s SQLiteDriver) ViewNames(schema string, whitelist, blacklist []string) ([]string, error) {
	query := `SELECT name FROM sqlite_master WHERE type='view'`
	args := []any{}

	if len(whitelist) > 0 {
		views := drivers.TablesFromList(whitelist)
		if len(views) > 0 {
			query += fmt.Sprintf(" and tbl_name in (%s)", strings.Repeat(",?", len(views))[1:])
			for _, w := range views {
				args = append(args, w)
			}
		}
	}

	if len(blacklist) > 0 {
		views := drivers.TablesFromList(blacklist)
		if len(views) > 0 {
			query += fmt.Sprintf(" and tbl_name not in (%s)", strings.Repeat(",?", len(views))[1:])
			for _, b := range views {
				args = append(args, b)
			}
		}
	}

	rows, err := s.dbConn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	var names []string
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "sqlite_sequence" {
			names = append(names, name)
		}
	}

	return names, nil
}

// ViewCapabilities return what actions are allowed for a view.
func (s SQLiteDriver) ViewCapabilities(schema, name string) (drivers.ViewCapabilities, error) {
	// Inserts may be allowed with the presence of an INSTEAD OF TRIGGER
	// but it is not yet implemented.
	// See: https://www.sqlite.org/lang_createview.html
	capabilities := drivers.ViewCapabilities{
		CanInsert: false,
		CanUpsert: false,
	}

	return capabilities, nil
}

func (s SQLiteDriver) ViewColumns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	return s.Columns(schema, tableName, whitelist, blacklist)
}

type sqliteIndex struct {
	SeqNum  int
	Unique  int
	Partial int
	Name    string
	Origin  string
	Columns []string
}

type sqliteTableInfo struct {
	Cid          string
	Name         string
	Type         string
	NotNull      bool
	DefaultValue *string
	Pk           int
	Hidden       int
}

func (s SQLiteDriver) tableInfo(tableName string) ([]*sqliteTableInfo, error) {
	var ret []*sqliteTableInfo
	rows, err := s.dbConn.Query(fmt.Sprintf("PRAGMA table_xinfo('%s')", tableName))

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		tinfo := &sqliteTableInfo{}
		if err := rows.Scan(&tinfo.Cid, &tinfo.Name, &tinfo.Type, &tinfo.NotNull, &tinfo.DefaultValue, &tinfo.Pk, &tinfo.Hidden); err != nil {
			return nil, fmt.Errorf("unable to scan for table %s: %w", tableName, err)
		}

		ret = append(ret, tinfo)
	}
	return ret, nil
}

func (s SQLiteDriver) indexes(tableName string) ([]*sqliteIndex, error) {
	var ret []*sqliteIndex
	rows, err := s.dbConn.Query(fmt.Sprintf("PRAGMA index_list('%s')", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var idx = &sqliteIndex{}
		var columns []string
		if err := rows.Scan(&idx.SeqNum, &idx.Name, &idx.Unique, &idx.Origin, &idx.Partial); err != nil {
			return nil, err
		}
		// get all columns stored within the index
		rowsColumns, err := s.dbConn.Query(fmt.Sprintf("PRAGMA index_info('%s')", idx.Name))
		if err != nil {
			return nil, err
		}
		for rowsColumns.Next() {
			var rankIndex, rankTable int
			var colName string
			if err := rowsColumns.Scan(&rankIndex, &rankTable, &colName); err != nil {
				return nil, fmt.Errorf("unable to scan for index %s: %w", idx.Name, err)
			}
			columns = append(columns, colName)
		}
		rowsColumns.Close()
		idx.Columns = columns
		ret = append(ret, idx)
	}
	return ret, nil
}

// Columns takes a table name and attempts to retrieve the table information
// from the database. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (s SQLiteDriver) Columns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	var columns []drivers.Column

	// get all indexes
	idxs, err := s.indexes(tableName)
	if err != nil {
		return nil, err
	}

	// finally get the remaining information about the columns
	tinfo, err := s.tableInfo(tableName)
	if err != nil {
		return nil, err
	}

	query := "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? AND sql LIKE '%AUTOINCREMENT%'"
	result, err := s.dbConn.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	tableHasAutoIncr := result.Next()
	if err := result.Close(); err != nil {
		return nil, err
	}

	var whiteColumns, blackColumns []string
	if len(whitelist) != 0 {
		whiteColumns = drivers.ColumnsFromList(whitelist, tableName)
	}
	if len(blacklist) != 0 {
		blackColumns = drivers.ColumnsFromList(blacklist, tableName)
	}

	nPkeys := 0
	for _, column := range tinfo {
		if column.Pk != 0 {
			nPkeys++
		}
	}

ColumnLoop:
	for _, column := range tinfo {
		if len(whiteColumns) != 0 {
			found := false
			for _, white := range whiteColumns {
				if white == column.Name {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		} else if len(blackColumns) != 0 {
			for _, black := range blackColumns {
				if black == column.Name {
					continue ColumnLoop
				}
			}
		}

		bColumn := drivers.Column{
			Name:       column.Name,
			FullDBType: strings.ToUpper(column.Type),
			DBType:     strings.ToUpper(column.Type),
			Nullable:   !column.NotNull,
		}

		// also get a correct information for Unique
		for _, idx := range idxs {
			// A unique index with multiple columns does not make
			// the individual column unique
			if len(idx.Columns) > 1 {
				continue
			}
			for _, name := range idx.Columns {
				if name == column.Name {
					// A column is unique if it has a unique non-partial index
					bColumn.Unique = idx.Unique > 0 && idx.Partial == 0
				}
			}
		}

		isPrimaryKeyInteger := column.Pk == 1 && bColumn.FullDBType == "INTEGER"
		// This is special behavior noted in the sqlite documentation.
		// An integer primary key becomes synonymous with the internal ROWID
		// and acts as an auto incrementing value. Although there's important
		// differences between using the keyword AUTOINCREMENT and this inferred
		// version, they don't matter here so just masquerade as the same thing as
		// above.
		autoIncr := isPrimaryKeyInteger && (tableHasAutoIncr || nPkeys == 1)

		// See: https://github.com/sqlite/sqlite/blob/91f621531dc1cb9ba5f6a47eb51b1de9ed8bdd07/src/pragma.c#L1165
		bColumn.AutoGenerated = autoIncr || column.Hidden == 2 || column.Hidden == 3

		if column.DefaultValue != nil {
			bColumn.Default = *column.DefaultValue
		} else if autoIncr {
			bColumn.Default = "auto_increment"
		} else if bColumn.AutoGenerated {
			bColumn.Default = "auto_generated"
		}

		if bColumn.Nullable && bColumn.Default == "" {
			bColumn.Default = "NULL"
		}

		columns = append(columns, bColumn)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (s SQLiteDriver) PrimaryKeyInfo(schema, tableName string) (*drivers.PrimaryKey, error) {
	// lookup the columns affected by the PK
	tinfo, err := s.tableInfo(tableName)
	if err != nil {
		return nil, err
	}

	var columns []string
	for _, column := range tinfo {
		if column.Pk > 0 {
			columns = append(columns, column.Name)
		}
	}

	var pk *drivers.PrimaryKey
	if len(columns) > 0 {
		pk = &drivers.PrimaryKey{Columns: columns}
	}
	return pk, nil
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (s SQLiteDriver) ForeignKeyInfo(schema, tableName string) ([]drivers.ForeignKey, error) {
	dbForeignKeys, err := s.foreignKeyInfoFromDB(schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("read foreign keys info from db: %w", err)
	}

	return drivers.CombineConfigAndDBForeignKeys(s.configForeignKeys, tableName, dbForeignKeys), nil
}
func (s SQLiteDriver) foreignKeyInfoFromDB(schema, tableName string) ([]drivers.ForeignKey, error) {
	var fkeys []drivers.ForeignKey

	query := fmt.Sprintf("PRAGMA foreign_key_list('%s')", tableName)

	var rows *sql.Rows
	var err error
	if rows, err = s.dbConn.Query(query, tableName); err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var fkey drivers.ForeignKey
		var onu, ond, match string
		var id, seq int

		fkey.Table = tableName
		err = rows.Scan(&id, &seq, &fkey.ForeignTable, &fkey.Column, &fkey.ForeignColumn, &onu, &ond, &match)
		if err != nil {
			return nil, err
		}
		fkey.Name = fmt.Sprintf("FK_%d", id)

		fkeys = append(fkeys, fkey)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return fkeys, nil
}

// TranslateColumnType converts sqlite database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
// https://sqlite.org/datatype3.html
func (SQLiteDriver) TranslateColumnType(c drivers.Column) drivers.Column {
	if c.Nullable {
		switch strings.Split(c.DBType, "(")[0] {
		case "INT", "INTEGER", "BIGINT":
			c.Type = "null.Int64"
		case "TINYINT", "INT8":
			c.Type = "null.Int8"
		case "SMALLINT", "INT2":
			c.Type = "null.Int16"
		case "MEDIUMINT":
			c.Type = "null.Int32"
		case "UNSIGNED BIG INT":
			c.Type = "null.Uint64"
		case "CHARACTER", "VARCHAR", "VARYING CHARACTER", "NCHAR",
			"NATIVE CHARACTER", "NVARCHAR", "TEXT", "CLOB":
			c.Type = "null.String"
		case "BLOB":
			c.Type = "null.Bytes"
		case "FLOAT":
			c.Type = "null.Float32"
		case "REAL", "DOUBLE", "DOUBLE PRECISION":
			c.Type = "null.Float64"
		case "NUMERIC", "DECIMAL":
			c.Type = "types.NullDecimal"
		case "BOOLEAN":
			c.Type = "null.Bool"
		case "DATE", "DATETIME", "TIMESTAMP":
			c.Type = "null.Time"
		case "JSON":
			c.Type = "null.JSON"

		default:
			c.Type = "null.String"
		}
	} else {
		switch strings.Split(c.DBType, "(")[0] {
		case "INT", "INTEGER", "BIGINT":
			c.Type = "int64"
		case "TINYINT", "INT8":
			c.Type = "int8"
		case "SMALLINT", "INT2":
			c.Type = "int16"
		case "MEDIUMINT":
			c.Type = "int32"
		case "UNSIGNED BIG INT":
			c.Type = "uint64"
		case "CHARACTER", "VARCHAR", "VARYING CHARACTER", "NCHAR",
			"NATIVE CHARACTER", "NVARCHAR", "TEXT", "CLOB":
			c.Type = "string"
		case "BLOB":
			c.Type = "[]byte"
		case "FLOAT":
			c.Type = "float32"
		case "REAL", "DOUBLE", "DOUBLE PRECISION":
			c.Type = "float64"
		case "NUMERIC", "DECIMAL":
			c.Type = "types.Decimal"
		case "BOOLEAN":
			c.Type = "bool"
		case "DATE", "DATETIME", "TIMESTAMP":
			c.Type = "time.Time"
		case "JSON":
			c.Type = "types.JSON"

		default:
			c.Type = "string"
		}
	}

	return c
}

// Imports returns important imports for the driver
func (SQLiteDriver) Imports() (col importers.Collection, err error) {
	col.All = importers.Set{
		Standard: importers.List{
			`"strconv"`,
		},
	}

	col.Singleton = importers.Map{
		"sqlite_upsert": {
			Standard: importers.List{
				`"fmt"`,
				`"strings"`,
			},
			ThirdParty: importers.List{
				`"github.com/aarondl/strmangle"`,
				`"github.com/aarondl/sqlboiler/v4/drivers"`,
			},
		},
	}

	col.TestSingleton = importers.Map{
		"sqlite3_suites_test": {
			Standard: importers.List{
				`"testing"`,
			},
		},
		"sqlite3_main_test": {
			Standard: importers.List{
				`"database/sql"`,
				`"fmt"`,
				`"io"`,
				`"math/rand"`,
				`"os"`,
				`"os/exec"`,
				`"path/filepath"`,
				`"regexp"`,
			},
			ThirdParty: importers.List{
				`"github.com/pkg/errors"`,
				`"github.com/spf13/viper"`,
				`_ "modernc.org/sqlite"`,
			},
		},
	}

	col.BasedOnType = importers.Map{
		"null.Float32": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Float64": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Int": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Int8": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Int16": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Int32": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Int64": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Uint": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Uint8": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Uint16": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Uint32": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Uint64": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.String": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Bool": {
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
		"types.Decimal": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.NullDecimal": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},

		"types.JSON": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"null.JSON": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
	}
	return col, err
}
