package driver

import (
	"database/sql"
	"embed"
	"encoding/base64"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/friendsofgo/errors"
	"github.com/go-sql-driver/mysql"
	"github.com/aarondl/strmangle"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/importers"
)

//go:embed override
var templates embed.FS

func init() {
	drivers.RegisterFromInit("mysql", &MySQLDriver{})
}

// Assemble is more useful for calling into the library so you don't
// have to instantiate an empty type.
func Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	driver := MySQLDriver{}
	return driver.Assemble(config)
}

// MySQLDriver holds the database connection string and a handle
// to the database connection.
type MySQLDriver struct {
	connStr           string
	conn              *sql.DB
	addEnumTypes      bool
	enumNullPrefix    string
	tinyIntAsInt      bool
	configForeignKeys []drivers.ForeignKey
}

// Templates that should be added/overridden
func (MySQLDriver) Templates() (map[string]string, error) {
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

// Assemble all the information we need to provide back to the driver
func (m *MySQLDriver) Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			dbinfo = nil
			err = r.(error)
		}
	}()

	user := config.MustString(drivers.ConfigUser)
	pass, _ := config.String(drivers.ConfigPass)
	dbname := config.MustString(drivers.ConfigDBName)
	host := config.MustString(drivers.ConfigHost)
	port := config.DefaultInt(drivers.ConfigPort, 3306)
	unixSocket := config.DefaultString(drivers.ConfigUnixSocket, "")
	// mysql ssl mode
	// see https://dev.mysql.com/doc/refman/5.7/en/connection-options.html#option_general_ssl-mode
	// could be one of PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
	sslMode := config.DefaultString(drivers.ConfigSSLMode, "true")

	schema := dbname
	whitelist, _ := config.StringSlice(drivers.ConfigWhitelist)
	blacklist, _ := config.StringSlice(drivers.ConfigBlacklist)
	concurrency := config.DefaultInt(drivers.ConfigConcurrency, drivers.DefaultConcurrency)

	tinyIntAsIntIntf, ok := config["tinyint_as_int"]
	if ok {
		if b, ok := tinyIntAsIntIntf.(bool); ok {
			m.tinyIntAsInt = b
		}
	}

	m.addEnumTypes, _ = config[drivers.ConfigAddEnumTypes].(bool)
	m.enumNullPrefix = strmangle.TitleCase(config.DefaultString(drivers.ConfigEnumNullPrefix, "Null"))

	if unixSocket != "" {
		m.connStr = MySQLBuildQueryStringUnixSocket(user, pass, dbname, unixSocket, sslMode)
	} else {
		m.connStr = MySQLBuildQueryString(user, pass, dbname, host, port, sslMode)
	}

	m.configForeignKeys = config.MustForeignKeys(drivers.ConfigForeignKeys)
	m.conn, err = sql.Open("mysql", m.connStr)
	if err != nil {
		return nil, errors.Wrap(err, "sqlboiler-mysql failed to connect to database")
	}

	defer func() {
		if e := m.conn.Close(); e != nil {
			dbinfo = nil
			err = e
		}
	}()

	dbinfo = &drivers.DBInfo{
		Dialect: drivers.Dialect{
			LQ: '`',
			RQ: '`',

			UseLastInsertID: true,
			UseSchema:       false,
		},
	}

	dbinfo.Tables, err = drivers.TablesConcurrently(m, schema, whitelist, blacklist, concurrency)
	if err != nil {
		return nil, err
	}

	return dbinfo, err
}

func MySQLBuildQueryStringUnixSocket(user, pass, dbname, unixSocket string, sslMode string) string {
	config := mysql.NewConfig()

	config.User = user
	if len(pass) != 0 {
		config.Passwd = pass
	}
	config.DBName = dbname
	config.Net = "unix"
	config.Addr = unixSocket
	config.TLSConfig = sslMode

	// MySQL is a bad, and by default reads date/datetime into a []byte
	// instead of a time.Time. Tell it to stop being a bad.
	config.ParseTime = true
	return config.FormatDSN()
}

// MySQLBuildQueryString builds a query string for MySQL.
func MySQLBuildQueryString(user, pass, dbname, host string, port int, sslMode string) string {
	config := mysql.NewConfig()

	config.User = user
	if len(pass) != 0 {
		config.Passwd = pass
	}
	config.DBName = dbname
	config.Net = "tcp"
	config.Addr = host
	if port == 0 {
		port = 3306
	}
	config.Addr += ":" + strconv.Itoa(port)
	config.TLSConfig = sslMode

	// MySQL is a bad, and by default reads date/datetime into a []byte
	// instead of a time.Time. Tell it to stop being a bad.
	config.ParseTime = true

	return config.FormatDSN()
}

// TableNames connects to the mysql database and
// retrieves all table names from the information_schema where the
// table schema is public.
func (m *MySQLDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := `select table_name from information_schema.tables where table_schema = ? and table_type = 'BASE TABLE'`
	args := []any{schema}
	if len(whitelist) > 0 {
		tables := drivers.TablesFromList(whitelist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name in (%s)", strings.Repeat(",?", len(tables))[1:])
			for _, w := range tables {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		tables := drivers.TablesFromList(blacklist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s)", strings.Repeat(",?", len(tables))[1:])
			for _, b := range tables {
				args = append(args, b)
			}
		}
	}

	query += ` order by table_name;`

	rows, err := m.conn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

// ViewNames connects to the postgres database and
// retrieves all view names from the information_schema where the
// view schema is schema. It uses a whitelist and blacklist.
func (m *MySQLDriver) ViewNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := `select table_name from information_schema.views where table_schema = ?`
	args := []any{schema}
	if len(whitelist) > 0 {
		tables := drivers.TablesFromList(whitelist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name in (%s)", strings.Repeat(",?", len(tables))[1:])
			for _, w := range tables {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		tables := drivers.TablesFromList(blacklist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s)", strings.Repeat(",?", len(tables))[1:])
			for _, b := range tables {
				args = append(args, b)
			}
		}
	}

	query += ` order by table_name;`

	rows, err := m.conn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

// ViewCapabilities return what actions are allowed for a view.
func (m *MySQLDriver) ViewCapabilities(schema, name string) (drivers.ViewCapabilities, error) {
	capabilities := drivers.ViewCapabilities{
		// No definite way to check if a view is insertable
		// See: https://dba.stackexchange.com/questions/285451/does-mysql-have-a-built-in-way-to-tell-whether-a-view-is-insertable-not-just-up?newreg=e6c571353a0948638bec10cf7f8c6f6f
		CanInsert: false,
		CanUpsert: false,
	}

	return capabilities, nil
}

func (m *MySQLDriver) ViewColumns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	return m.Columns(schema, tableName, whitelist, blacklist)
}

// Columns takes a table name and attempts to retrieve the table information
// from the database information_schema.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (m *MySQLDriver) Columns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	var columns []drivers.Column
	args := []any{tableName, tableName, schema, schema, schema, schema, tableName, tableName, schema}

	query := `
	select
	c.column_name,
	c.column_type,
	c.column_comment,
	if(c.data_type = 'enum', c.column_type, c.data_type),
	if(extra = 'auto_increment','auto_increment',
		if(version() like '%MariaDB%' and c.column_default = 'NULL', '',
		if(version() like '%MariaDB%' and c.data_type in ('varchar','char','binary','date','datetime','time'),
			replace(substring(c.column_default,2,length(c.column_default)-2),'\'\'','\''),
				c.column_default))),
	c.is_nullable = 'YES',
	(c.extra = 'STORED GENERATED' OR c.extra = 'VIRTUAL GENERATED') is_generated,
		exists (
			select c.column_name
			from information_schema.table_constraints tc
			inner join information_schema.key_column_usage kcu
				on tc.constraint_name = kcu.constraint_name
			where tc.table_name = ? and kcu.table_name = ? and tc.table_schema = ? and kcu.table_schema = ? and
				c.column_name = kcu.column_name and
				(tc.constraint_type = 'PRIMARY KEY' or tc.constraint_type = 'UNIQUE') and
				(select count(*) from information_schema.key_column_usage where table_schema = ? and
				constraint_schema = ? and table_name = ? and constraint_name = tc.constraint_name) = 1
		) as is_unique
	from information_schema.columns as c
	where table_name = ? and table_schema = ?`

	if len(whitelist) > 0 {
		cols := drivers.ColumnsFromList(whitelist, tableName)
		if len(cols) > 0 {
			query += fmt.Sprintf(" and c.column_name in (%s)", strings.Repeat(",?", len(cols))[1:])
			for _, w := range cols {
				args = append(args, w)
			}
		}
	}
	
	if len(blacklist) > 0 {
		cols := drivers.ColumnsFromList(blacklist, tableName)
		if len(cols) > 0 {
			query += fmt.Sprintf(" and c.column_name not in (%s)", strings.Repeat(",?", len(cols))[1:])
			for _, w := range cols {
				args = append(args, w)
			}
		}
	}

	query += ` order by c.ordinal_position;`

	rows, err := m.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colFullType, colComment, colType string
		var nullable, generated, unique bool
		var defaultValue *string
		if err := rows.Scan(&colName, &colFullType, &colComment, &colType, &defaultValue, &nullable, &generated, &unique); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}

		column := drivers.Column{
			Name:          colName,
			Comment:       colComment,
			FullDBType:    colFullType, // example: tinyint(1) instead of tinyint
			DBType:        colType,
			Nullable:      nullable,
			Unique:        unique,
			AutoGenerated: generated,
		}

		if defaultValue != nil {
			column.Default = *defaultValue
		}

		// A generated column technically has a default value
		if column.Default == "" && column.AutoGenerated {
			column.Default = "AUTO_GENERATED"
		}

		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (m *MySQLDriver) PrimaryKeyInfo(schema, tableName string) (*drivers.PrimaryKey, error) {
	pkey := &drivers.PrimaryKey{}
	var err error

	query := `
	select tc.constraint_name
	from information_schema.table_constraints as tc
	where tc.table_name = ? and tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = ?;`

	row := m.conn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	queryColumns := `
	select kcu.column_name
	from   information_schema.key_column_usage as kcu
	where  table_name = ? and constraint_name = ? and table_schema = ?
	order by kcu.ordinal_position;`

	var rows *sql.Rows
	if rows, err = m.conn.Query(queryColumns, tableName, pkey.Name, schema); err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string

		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	pkey.Columns = columns

	return pkey, nil
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (m *MySQLDriver) ForeignKeyInfo(schema, tableName string) ([]drivers.ForeignKey, error) {
	dbForeignKeys, err := m.foreignKeyInfoFromDB(schema, tableName)
	if err != nil {
		return nil, errors.Wrap(err, "read foreign keys info from db")
	}

	return drivers.CombineConfigAndDBForeignKeys(m.configForeignKeys, tableName, dbForeignKeys), nil
}
func (m *MySQLDriver) foreignKeyInfoFromDB(schema, tableName string) ([]drivers.ForeignKey, error) {
	var fkeys []drivers.ForeignKey

	query := `
	select constraint_name, table_name, column_name, referenced_table_name, referenced_column_name
	from information_schema.key_column_usage
	where table_schema = ? and referenced_table_schema = ? and table_name = ?
	order by constraint_name, table_name, column_name, referenced_table_name, referenced_column_name
	`

	var rows *sql.Rows
	var err error
	if rows, err = m.conn.Query(query, schema, schema, tableName); err != nil {
		return nil, err
	}

	for rows.Next() {
		var fkey drivers.ForeignKey
		var sourceTable string

		fkey.Table = tableName
		err = rows.Scan(&fkey.Name, &sourceTable, &fkey.Column, &fkey.ForeignTable, &fkey.ForeignColumn)
		if err != nil {
			return nil, err
		}

		fkeys = append(fkeys, fkey)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return fkeys, nil
}

// TranslateColumnType converts mysql database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
// Deprecated: for MySQL enum types to be created properly TranslateTableColumnType method should be used instead.
func (m *MySQLDriver) TranslateColumnType(drivers.Column) drivers.Column {
	panic("TranslateTableColumnType should be called")
}

// TranslateTableColumnType converts mysql database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
func (m *MySQLDriver) TranslateTableColumnType(c drivers.Column, tableName string) drivers.Column {
	unsigned := strings.Contains(c.FullDBType, "unsigned")
	if c.Nullable {
		switch c.DBType {
		case "tinyint":
			// map tinyint(1) to bool if TinyintAsBool is true
			if !m.tinyIntAsInt && c.FullDBType == "tinyint(1)" {
				c.Type = "null.Bool"
			} else if unsigned {
				c.Type = "null.Uint8"
			} else {
				c.Type = "null.Int8"
			}
		case "smallint":
			if unsigned {
				c.Type = "null.Uint16"
			} else {
				c.Type = "null.Int16"
			}
		case "mediumint":
			if unsigned {
				c.Type = "null.Uint32"
			} else {
				c.Type = "null.Int32"
			}
		case "int", "integer":
			if unsigned {
				c.Type = "null.Uint"
			} else {
				c.Type = "null.Int"
			}
		case "bigint":
			if unsigned {
				c.Type = "null.Uint64"
			} else {
				c.Type = "null.Int64"
			}
		case "float":
			c.Type = "null.Float32"
		case "double", "double precision", "real":
			c.Type = "null.Float64"
		case "boolean", "bool":
			c.Type = "null.Bool"
		case "date", "datetime", "timestamp":
			c.Type = "null.Time"
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			c.Type = "null.Bytes"
		case "numeric", "decimal", "dec", "fixed":
			c.Type = "types.NullDecimal"
		case "json":
			c.Type = "null.JSON"
		default:
			if len(strmangle.ParseEnumVals(c.DBType)) > 0 && m.addEnumTypes {
				c.Type = strmangle.TitleCase(tableName) + m.enumNullPrefix + strmangle.TitleCase(c.Name)
			} else {
				c.Type = "null.String"
			}
		}
	} else {
		switch c.DBType {
		case "tinyint":
			// map tinyint(1) to bool if TinyintAsBool is true
			if !m.tinyIntAsInt && c.FullDBType == "tinyint(1)" {
				c.Type = "bool"
			} else if unsigned {
				c.Type = "uint8"
			} else {
				c.Type = "int8"
			}
		case "smallint":
			if unsigned {
				c.Type = "uint16"
			} else {
				c.Type = "int16"
			}
		case "mediumint":
			if unsigned {
				c.Type = "uint32"
			} else {
				c.Type = "int32"
			}
		case "int", "integer":
			if unsigned {
				c.Type = "uint"
			} else {
				c.Type = "int"
			}
		case "bigint":
			if unsigned {
				c.Type = "uint64"
			} else {
				c.Type = "int64"
			}
		case "float":
			c.Type = "float32"
		case "double", "double precision", "real":
			c.Type = "float64"
		case "boolean", "bool":
			c.Type = "bool"
		case "date", "datetime", "timestamp":
			c.Type = "time.Time"
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			c.Type = "[]byte"
		case "numeric", "decimal", "dec", "fixed":
			c.Type = "types.Decimal"
		case "json":
			c.Type = "types.JSON"
		default:
			if len(strmangle.ParseEnumVals(c.DBType)) > 0 && m.addEnumTypes {
				c.Type = strmangle.TitleCase(tableName) + strmangle.TitleCase(c.Name)
			} else {
				c.Type = "string"
			}
		}
	}

	return c
}

// Imports returns important imports for the driver
func (MySQLDriver) Imports() (col importers.Collection, err error) {
	col.All = importers.Set{
		Standard: importers.List{
			`"strconv"`,
		},
	}

	col.Singleton = importers.Map{
		"mysql_upsert": {
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
		"mysql_suites_test": {
			Standard: importers.List{
				`"testing"`,
			},
		},
		"mysql_main_test": {
			Standard: importers.List{
				`"bytes"`,
				`"database/sql"`,
				`"fmt"`,
				`"io"`,
				`"os"`,
				`"os/exec"`,
				`"regexp"`,
				`"strings"`,
			},
			ThirdParty: importers.List{
				`"github.com/kat-co/vala"`,
				`"github.com/friendsofgo/errors"`,
				`"github.com/spf13/viper"`,
				`"github.com/aarondl/sqlboiler/v4/drivers/sqlboiler-mysql/driver"`,
				`"github.com/aarondl/randomize"`,
				`_ "github.com/go-sql-driver/mysql"`,
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
		"null.JSON": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},

		"time.Time": {
			Standard: importers.List{`"time"`},
		},
		"types.JSON": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.Decimal": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.NullDecimal": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
	}
	return col, err
}