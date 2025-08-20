// Package driver implements an sqlboiler driver.
// It can be used by either building the main.go in the same project
// and using as a binary or using the side effect import.
package driver

import (
	"database/sql"
	"embed"
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"

	"github.com/aarondl/sqlboiler/v4/importers"

	"github.com/aarondl/strmangle"
	"github.com/friendsofgo/errors"

	"github.com/aarondl/sqlboiler/v4/drivers"

	// Side-effect import sql driver
	_ "github.com/lib/pq"
)

//go:embed override
var templates embed.FS

func init() {
	drivers.RegisterFromInit("psql", &PostgresDriver{})
}

// Assemble is more useful for calling into the library so you don't
// have to instantiate an empty type.
func Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
	driver := PostgresDriver{}
	return driver.Assemble(config)
}

// PostgresDriver holds the database connection string and a handle
// to the database connection.
type PostgresDriver struct {
	connStr        string
	conn           *sql.DB
	version        int
	addEnumTypes   bool
	enumNullPrefix string

	uniqueColumns     *sync.Map
	configForeignKeys []drivers.ForeignKey
}

type columnIdentifier struct {
	Schema string
	Table  string
	Column string
}

// Templates that should be added/overridden
func (p *PostgresDriver) Templates() (map[string]string, error) {
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
func (p *PostgresDriver) Assemble(config drivers.Config) (dbinfo *drivers.DBInfo, err error) {
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
	port := config.DefaultInt(drivers.ConfigPort, 5432)
	sslmode := config.DefaultString(drivers.ConfigSSLMode, "require")
	schema := config.DefaultString(drivers.ConfigSchema, "public")
	noOutputSchema := config.DefaultBool(drivers.ConfigNoOutputSchema, false)
	whitelist, _ := config.StringSlice(drivers.ConfigWhitelist)
	blacklist, _ := config.StringSlice(drivers.ConfigBlacklist)
	concurrency := config.DefaultInt(drivers.ConfigConcurrency, drivers.DefaultConcurrency)

	switch {
	case noOutputSchema:
		break
	case schema == "public":
		noOutputSchema = true
	default:
		// just to be explicit, even though it's the default in the getter
		noOutputSchema = false
	}

	p.addEnumTypes, _ = config[drivers.ConfigAddEnumTypes].(bool)
	p.enumNullPrefix = strmangle.TitleCase(config.DefaultString(drivers.ConfigEnumNullPrefix, "Null"))
	p.connStr = PSQLBuildQueryString(user, pass, dbname, host, port, sslmode)
	p.configForeignKeys = config.MustForeignKeys(drivers.ConfigForeignKeys)
	p.conn, err = sql.Open("postgres", p.connStr)
	if err != nil {
		return nil, errors.Wrap(err, "sqlboiler-psql failed to connect to database")
	}

	defer func() {
		if e := p.conn.Close(); e != nil {
			dbinfo = nil
			err = e
		}
	}()

	p.version, err = p.getVersion()
	if err != nil {
		return nil, errors.Wrap(err, "sqlboiler-psql failed to get database version")
	}

	if err = p.loadUniqueColumns(); err != nil {
		return nil, errors.Wrap(err, "sqlboiler-psql failed to load unique columns")
	}

	dbinfo = &drivers.DBInfo{
		Schema: schema,
		Dialect: drivers.Dialect{
			LQ: '"',
			RQ: '"',

			UseIndexPlaceholders: true,
			UseSchema:            !noOutputSchema,
			UseDefaultKeyword:    true,
		},
	}
	dbinfo.Tables, err = drivers.TablesConcurrently(p, schema, whitelist, blacklist, concurrency)
	if err != nil {
		return nil, err
	}

	return dbinfo, err
}

// PSQLBuildQueryString builds a query string.
func PSQLBuildQueryString(user, pass, dbname, host string, port int, sslmode string) string {
	parts := []string{}
	if len(user) != 0 {
		parts = append(parts, fmt.Sprintf("user=%s", user))
	}
	if len(pass) != 0 {
		parts = append(parts, fmt.Sprintf("password=%s", pass))
	}
	if len(dbname) != 0 {
		parts = append(parts, fmt.Sprintf("dbname=%s", dbname))
	}
	if len(host) != 0 {
		parts = append(parts, fmt.Sprintf("host=%s", host))
	}
	if port != 0 {
		parts = append(parts, fmt.Sprintf("port=%d", port))
	}
	if len(sslmode) != 0 {
		parts = append(parts, fmt.Sprintf("sslmode=%s", sslmode))
	}

	return strings.Join(parts, " ")
}

// TableNames connects to the postgres database and
// retrieves all table names from the information_schema where the
// table schema is schema. It uses a whitelist and blacklist.
func (p *PostgresDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := `select table_name from information_schema.tables where table_schema = $1 and table_type = 'BASE TABLE'`
	args := []any{schema}
	if len(whitelist) > 0 {
		tables := drivers.TablesFromList(whitelist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name in (%s)", strmangle.Placeholders(true, len(tables), 2, 1))
			for _, w := range tables {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		tables := drivers.TablesFromList(blacklist)
		if len(tables) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s)", strmangle.Placeholders(true, len(tables), 2, 1))
			for _, b := range tables {
				args = append(args, b)
			}
		}
	}

	query += ` order by table_name;`

	rows, err := p.conn.Query(query, args...)
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

	return names, nil
}

// ViewNames connects to the postgres database and
// retrieves all view names from the information_schema where the
// view schema is schema. It uses a whitelist and blacklist.
func (p *PostgresDriver) ViewNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string

	query := `select 
		table_name 
	from (
			select 
				table_name, 
				table_schema 
			from information_schema.views
			UNION
			select 
				matviewname as table_name, 
				schemaname as table_schema 
			from pg_matviews 
	) as v where v.table_schema= $1`
	args := []any{schema}
	if len(whitelist) > 0 {
		views := drivers.TablesFromList(whitelist)
		if len(views) > 0 {
			query += fmt.Sprintf(" and table_name in (%s)", strmangle.Placeholders(true, len(views), 2, 1))
			for _, w := range views {
				args = append(args, w)
			}
		}
	} else if len(blacklist) > 0 {
		views := drivers.TablesFromList(blacklist)
		if len(views) > 0 {
			query += fmt.Sprintf(" and table_name not in (%s)", strmangle.Placeholders(true, len(views), 2, 1))
			for _, b := range views {
				args = append(args, b)
			}
		}
	}

	query += ` order by table_name;`

	rows, err := p.conn.Query(query, args...)
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

	return names, nil
}

// ViewCapabilities return what actions are allowed for a view.
func (p *PostgresDriver) ViewCapabilities(schema, name string) (drivers.ViewCapabilities, error) {
	capabilities := drivers.ViewCapabilities{}

	query := `select 
		is_insertable_into,
		is_updatable,
		is_trigger_insertable_into,
		is_trigger_updatable,
		is_trigger_deletable
	from (
		select
			table_schema,
			table_name,
			is_insertable_into = 'YES' as is_insertable_into,
			is_updatable = 'YES' as is_updatable,
			is_trigger_insertable_into = 'YES' as is_trigger_insertable_into,
			is_trigger_updatable = 'YES' as is_trigger_updatable,
			is_trigger_deletable = 'YES' as is_trigger_deletable
		from information_schema.views
		UNION
		select 
			schemaname as table_schema,
			matviewname as table_name, 
			false as is_insertable_into,
			false as is_updatable,
			false as is_trigger_insertable_into,
			false as is_trigger_updatable, 
			false as is_trigger_deletable
		from pg_matviews 
	) as v where v.table_schema= $1 and v.table_name = $2 
	order by table_name;`

	row := p.conn.QueryRow(query, schema, name)

	var insertable, updatable, trInsert, trUpdate, trDelete bool
	if err := row.Scan(&insertable, &updatable, &trInsert, &trUpdate, &trDelete); err != nil {
		return capabilities, err
	}

	capabilities.CanInsert = insertable || trInsert
	capabilities.CanUpsert = insertable && updatable

	return capabilities, nil
}

// loadUniqueColumns is responsible for populating p.uniqueColumns with an entry
// for every table or view column that is made unique by an index or constraint.
// This information is queried once, rather than for each table, for performance
// reasons.
func (p *PostgresDriver) loadUniqueColumns() error {
	if p.uniqueColumns != nil {
		return nil
	}
	p.uniqueColumns = &sync.Map{}
	query := `with
method_a as (
    select
        tc.table_schema as schema_name,
        ccu.table_name as table_name,
        ccu.column_name as column_name
    from information_schema.table_constraints tc
    inner join information_schema.constraint_column_usage as ccu
        on tc.constraint_name = ccu.constraint_name
    where
        tc.constraint_type = 'UNIQUE' and (
            (select count(*)
            from information_schema.constraint_column_usage
            where constraint_schema = tc.table_schema and constraint_name = tc.constraint_name
            ) = 1
        )
),
method_b as (
    select
        pgix.schemaname as schema_name,
        pgix.tablename as table_name,
        pga.attname as column_name
    from pg_indexes pgix
    inner join pg_class pgc on pgix.indexname = pgc.relname and pgc.relkind = 'i' and pgc.relnatts = 1
    inner join pg_index pgi on pgi.indexrelid = pgc.oid
    inner join pg_attribute pga on pga.attrelid = pgi.indrelid and pga.attnum = ANY(pgi.indkey)
    where pgi.indisunique = true and pgi.indpred is null
),
results as (
    select * from method_a
    union
    select * from method_b
)
select * from results;
`
	rows, err := p.conn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var c columnIdentifier
		if err := rows.Scan(&c.Schema, &c.Table, &c.Column); err != nil {
			return errors.Wrapf(err, "unable to scan unique entry row")
		}
		p.uniqueColumns.Store(c, struct{}{})
	}
	return nil
}

func (p *PostgresDriver) ViewColumns(schema, tableName string, whitelist, blacklist []string) (
	[]drivers.Column,
	error,
) {
	return p.Columns(schema, tableName, whitelist, blacklist)
}

// Columns takes a table name and attempts to retrieve the table information
// from the database information_schema.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
func (p *PostgresDriver) Columns(schema, tableName string, whitelist, blacklist []string) ([]drivers.Column, error) {
	var columns []drivers.Column
	args := []any{schema, tableName}

	matviewQuery := `WITH cte_pg_attribute AS (
		SELECT
			pg_catalog.format_type(a.atttypid, NULL) LIKE '%[]' = TRUE as is_array,
			pg_catalog.format_type(a.atttypid, a.atttypmod) as column_full_type,
			a.*
		FROM pg_attribute a
	), cte_pg_namespace AS (
		SELECT
			n.nspname NOT IN ('pg_catalog', 'information_schema') = TRUE as is_user_defined,
			n.oid
		FROM pg_namespace n
	), cte_information_schema_domains AS (
		SELECT
			domain_name IS NOT NULL = TRUE as is_domain,
			data_type LIKE '%[]' = TRUE as is_array,
			domain_name,
			udt_name,
			data_type
		FROM information_schema.domains
	)
	SELECT 
		a.attnum as ordinal_position,
		a.attname as column_name,
		(
			case 
			when t.typtype = 'e'
			then (
				select 'enum.' || t.typname || '(''' || string_agg(labels.label, ''',''') || ''')'
				from (
					select pg_enum.enumlabel as label
					from pg_enum
					where pg_enum.enumtypid =
					(
						select typelem
						from pg_type
						inner join pg_namespace ON pg_type.typnamespace = pg_namespace.oid
						where pg_type.typtype = 'b' and pg_type.typname = ('_' || t.typname) and pg_namespace.nspname=$1
						limit 1
					)
					order by pg_enum.enumsortorder
				) as labels
			)
			when a.is_array OR d.is_array
			then 'ARRAY'
			when d.is_domain
			then d.data_type
			when tn.is_user_defined
			then 'USER-DEFINED'
			else pg_catalog.format_type(a.atttypid, NULL)
			end
		) as column_type,
		(
			case 
			when d.is_domain
			then d.udt_name		
			when a.column_full_type LIKE '%(%)%' AND t.typcategory IN ('S', 'V')
			then a.column_full_type
			else t.typname
			end
		) as column_full_type,
		(
			case 
			when d.is_domain
			then d.udt_name		
			else t.typname
			end
		) as udt_name,
		(
			case when a.is_array
			then
				case when tn.is_user_defined
				then 'USER-DEFINED'
				else RTRIM(pg_catalog.format_type(a.atttypid, NULL), '[]')
				end
			else NULL
			end
		) as array_type,
		d.domain_name,
		NULL as column_default,
		'' as column_comment,
		a.attnotnull = FALSE as is_nullable,
		FALSE as is_generated,
		a.attidentity <> '' as is_identity
	FROM cte_pg_attribute a
		JOIN pg_class c on a.attrelid = c.oid
		JOIN pg_namespace cn on c.relnamespace = cn.oid
		JOIN pg_type t ON t.oid = a.atttypid
		LEFT JOIN cte_pg_namespace tn ON t.typnamespace = tn.oid
		LEFT JOIN cte_information_schema_domains d ON d.domain_name = pg_catalog.format_type(a.atttypid, NULL)
		WHERE a.attnum > 0 
		AND c.relkind = 'm'
		AND NOT a.attisdropped
		AND c.relname = $2
		AND cn.nspname = $1`

	tableQuery := `
	select
		c.ordinal_position,
		c.column_name,
		ct.column_type,
		(
			case when c.character_maximum_length != 0
			then
			(
				ct.column_type || '(' || c.character_maximum_length || ')'
			)
			else c.udt_name
			end
		) as column_full_type,

		c.udt_name,
		(
			SELECT
				data_type
			FROM
				information_schema.element_types e
			WHERE
				c.table_catalog = e.object_catalog
				AND c.table_schema = e.object_schema
				AND c.table_name = e.object_name
				AND 'TABLE' = e.object_type
				AND c.dtd_identifier = e.collection_type_identifier
		) AS array_type,
		c.domain_name,
		c.column_default,

		COALESCE(col_description(('"'||c.table_schema||'"."'||c.table_name||'"')::regclass::oid, ordinal_position), '') as column_comment,

		c.is_nullable = 'YES' as is_nullable,
		(
				case when c.is_generated = 'ALWAYS' or c.identity_generation = 'ALWAYS'
				then TRUE else FALSE end
		) as is_generated,
		(case
			when (select
		    case
			    when column_name = 'is_identity' then (select c.is_identity = 'YES' as is_identity)
		    else
			    false
		    end as is_identity from information_schema.columns
		    WHERE table_schema='information_schema' and table_name='columns' and column_name='is_identity') IS NULL then 'NO' else is_identity end
		) = 'YES' as is_identity

		from information_schema.columns as c
		inner join pg_namespace as pgn on pgn.nspname = c.udt_schema
		left join pg_type pgt on c.data_type = 'USER-DEFINED' and pgn.oid = pgt.typnamespace and c.udt_name = pgt.typname,
		lateral (select
			(
				case when pgt.typtype = 'e'
				then
				(
					select 'enum.' || c.udt_name || '(''' || string_agg(labels.label, ''',''') || ''')'
					from (
						select pg_enum.enumlabel as label
						from pg_enum
						where pg_enum.enumtypid =
						(
							select typelem
							from pg_type
							inner join pg_namespace ON pg_type.typnamespace = pg_namespace.oid
							where pg_type.typtype = 'b' and pg_type.typname = ('_' || c.udt_name) and pg_namespace.nspname=$1
							limit 1
						)
						order by pg_enum.enumsortorder
					) as labels
				)
				else c.data_type
				end
			) as column_type
		) ct
		where c.table_name = $2 and c.table_schema = $1`

	query := fmt.Sprintf(`SELECT 
		column_name,
		COALESCE(column_type, column_full_type) as column_type,
		column_full_type,
		udt_name,
		array_type,
		domain_name,
		column_default,
		column_comment,
		is_nullable,
		is_generated,
		is_identity
	FROM (
		%s
		UNION
		%s
	) AS c`, matviewQuery, tableQuery)

	if len(whitelist) > 0 {
		cols := drivers.ColumnsFromList(whitelist, tableName)
		if len(cols) > 0 {
			query += fmt.Sprintf(" where c.column_name in (%s)", strmangle.Placeholders(true, len(cols), 3, 1))
			for _, w := range cols {
				args = append(args, w)
			}
		}
	}

	if len(blacklist) > 0 {
		cols := drivers.ColumnsFromList(blacklist, tableName)
		if len(cols) > 0 {
			query += fmt.Sprintf(" where c.column_name not in (%s)", strmangle.Placeholders(true, len(cols), 3, 1))
			for _, w := range cols {
				args = append(args, w)
			}
		}
	}

	query += ` order by c.ordinal_position;`

	rows, err := p.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var colName, colType, colFullType, udtName, comment string
		var defaultValue, arrayType, domainName *string
		var nullable, generated, identity bool
		if err := rows.Scan(&colName, &colType, &colFullType, &udtName, &arrayType, &domainName, &defaultValue, &comment, &nullable, &generated, &identity); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}
		_, unique := p.uniqueColumns.Load(columnIdentifier{schema, tableName, colName})
		column := drivers.Column{
			Name:          colName,
			DBType:        colType,
			FullDBType:    colFullType,
			ArrType:       arrayType,
			DomainName:    domainName,
			UDTName:       udtName,
			Comment:       comment,
			Nullable:      nullable,
			AutoGenerated: generated,
			Unique:        unique,
		}
		if defaultValue != nil {
			column.Default = *defaultValue
		}

		if identity {
			column.Default = "IDENTITY"
		}

		// A generated column technically has a default value
		if generated && column.Default == "" {
			column.Default = "GENERATED"
		}

		// A nullable column can always default to NULL
		if nullable && column.Default == "" {
			column.Default = "NULL"
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (p *PostgresDriver) PrimaryKeyInfo(schema, tableName string) (*drivers.PrimaryKey, error) {
	pkey := &drivers.PrimaryKey{}
	var err error

	query := `
	select tc.constraint_name
	from information_schema.table_constraints as tc
	where tc.table_name = $1 and tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = $2;`

	row := p.conn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	queryColumns := `
	select kcu.column_name
	from   information_schema.key_column_usage as kcu
	where  constraint_name = $1 and table_name = $2 and table_schema = $3
	order by kcu.ordinal_position;`

	var rows *sql.Rows
	if rows, err = p.conn.Query(queryColumns, pkey.Name, tableName, schema); err != nil {
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
func (p *PostgresDriver) ForeignKeyInfo(schema, tableName string) ([]drivers.ForeignKey, error) {
	dbForeignKeys, err := p.foreignKeyInfoFromDB(schema, tableName)
	if err != nil {
		return nil, errors.Wrap(err, "read foreign keys info from db")
	}

	return drivers.CombineConfigAndDBForeignKeys(p.configForeignKeys, tableName, dbForeignKeys), nil
}
func (p *PostgresDriver) foreignKeyInfoFromDB(schema, tableName string) ([]drivers.ForeignKey, error) {
	var fkeys []drivers.ForeignKey

	whereConditions := []string{"pgn.nspname = $2", "pgc.relname = $1", "pgcon.contype = 'f'"}
	if p.version >= 120000 {
		whereConditions = append(whereConditions, "pgasrc.attgenerated = ''", "pgadst.attgenerated = ''")
	}

	query := fmt.Sprintf(`
	select
		pgcon.conname,
		pgc.relname as source_table,
		pgasrc.attname as source_column,
		dstlookupname.relname as dest_table,
		pgadst.attname as dest_column
	from pg_namespace pgn
		inner join pg_class pgc on pgn.oid = pgc.relnamespace and pgc.relkind = 'r'
		inner join pg_constraint pgcon on pgn.oid = pgcon.connamespace and pgc.oid = pgcon.conrelid
		inner join pg_class dstlookupname on pgcon.confrelid = dstlookupname.oid and pgn.oid = dstlookupname.relnamespace
		inner join pg_attribute pgasrc on pgc.oid = pgasrc.attrelid and pgasrc.attnum = ANY(pgcon.conkey)
		inner join pg_attribute pgadst on pgcon.confrelid = pgadst.attrelid and pgadst.attnum = ANY(pgcon.confkey)
	where %s
	order by pgcon.conname, source_table, source_column, dest_table, dest_column`,
		strings.Join(whereConditions, " and "),
	)

	var rows *sql.Rows
	var err error
	if rows, err = p.conn.Query(query, tableName, schema); err != nil {
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

// TranslateColumnType converts postgres database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
func (p *PostgresDriver) TranslateColumnType(c drivers.Column) drivers.Column {
	if c.Nullable {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "null.Int64"
		case "integer", "serial":
			c.Type = "null.Int"
		case "oid":
			c.Type = "null.Uint32"
		case "smallint", "smallserial":
			c.Type = "null.Int16"
		case "decimal", "numeric":
			c.Type = "types.NullDecimal"
		case "double precision":
			c.Type = "null.Float64"
		case "real":
			c.Type = "null.Float32"
		case "bit", "interval", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			c.Type = "null.String"
		case `"char"`:
			c.Type = "null.Byte"
		case "bytea":
			c.Type = "null.Bytes"
		case "json", "jsonb":
			c.Type = "null.JSON"
		case "boolean":
			c.Type = "null.Bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone":
			c.Type = "null.Time"
		case "point":
			c.Type = "pgeo.NullPoint"
		case "line":
			c.Type = "pgeo.NullLine"
		case "lseg":
			c.Type = "pgeo.NullLseg"
		case "box":
			c.Type = "pgeo.NullBox"
		case "path":
			c.Type = "pgeo.NullPath"
		case "polygon":
			c.Type = "pgeo.NullPolygon"
		case "circle":
			c.Type = "pgeo.NullCircle"
		case "ARRAY":
			var dbType string
			c.Type, dbType = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType += dbType
		case "USER-DEFINED":
			switch c.UDTName {
			case "hstore":
				c.Type = "types.HStore"
				c.DBType = "hstore"
			case "citext":
				c.Type = "null.String"
			default:
				c.Type = "string"
				fmt.Fprintf(os.Stderr, "warning: incompatible data type detected: %s\n", c.UDTName)
			}
		default:
			if enumName := strmangle.ParseEnumName(c.DBType); enumName != "" && p.addEnumTypes {
				c.Type = p.enumNullPrefix + strmangle.TitleCase(enumName)
			} else {
				c.Type = "null.String"
			}
		}
	} else {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "int64"
		case "integer", "serial":
			c.Type = "int"
		case "oid":
			c.Type = "uint32"
		case "smallint", "smallserial":
			c.Type = "int16"
		case "decimal", "numeric":
			c.Type = "types.Decimal"
		case "double precision":
			c.Type = "float64"
		case "real":
			c.Type = "float32"
		case "bit", "interval", "uuint", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			c.Type = "string"
		case `"char"`:
			c.Type = "types.Byte"
		case "json", "jsonb":
			c.Type = "types.JSON"
		case "bytea":
			c.Type = "[]byte"
		case "boolean":
			c.Type = "bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone":
			c.Type = "time.Time"
		case "point":
			c.Type = "pgeo.Point"
		case "line":
			c.Type = "pgeo.Line"
		case "lseg":
			c.Type = "pgeo.Lseg"
		case "box":
			c.Type = "pgeo.Box"
		case "path":
			c.Type = "pgeo.Path"
		case "polygon":
			c.Type = "pgeo.Polygon"
		case "circle":
			c.Type = "pgeo.Circle"
		case "ARRAY":
			var dbType string
			c.Type, dbType = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType += dbType
		case "USER-DEFINED":
			switch c.UDTName {
			case "hstore":
				c.Type = "types.HStore"
				c.DBType = "hstore"
			case "citext":
				c.Type = "string"
			default:
				c.Type = "string"
				fmt.Fprintf(os.Stderr, "warning: incompatible data type detected: %s\n", c.UDTName)
			}
		default:
			if enumName := strmangle.ParseEnumName(c.DBType); enumName != "" && p.addEnumTypes {
				c.Type = strmangle.TitleCase(enumName)
			} else {
				c.Type = "string"
			}
		}
	}

	return c
}

// getArrayType returns the correct boil.Array type for each database type
func getArrayType(c drivers.Column) (string, string) {
	// If a domain is created with a statement like this: "CREATE DOMAIN
	// text_array AS TEXT[] CHECK ( ... )" then the array type will be null,
	// but the udt name will be whatever the underlying type is with a leading
	// underscore. Note that this code handles some types, but not nearly all
	// the possibities. Notably, an array of a user-defined type ("CREATE
	// DOMAIN my_array AS my_type[]") will be treated as an array of strings,
	// which is not guaranteed to be correct.
	if c.ArrType != nil {
		switch *c.ArrType {
		case "bigint", "bigserial", "integer", "serial", "smallint", "smallserial", "oid":
			return "types.Int64Array", *c.ArrType
		case "bytea":
			return "types.BytesArray", *c.ArrType
		case "bit", "interval", "uuint", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			return "types.StringArray", *c.ArrType
		case "boolean":
			return "types.BoolArray", *c.ArrType
		case "decimal", "numeric":
			return "types.DecimalArray", *c.ArrType
		case "double precision", "real":
			return "types.Float64Array", *c.ArrType
		default:
			return "types.StringArray", *c.ArrType
		}
	} else {
		switch c.UDTName {
		case "_int4", "_int8":
			return "types.Int64Array", c.UDTName
		case "_bytea":
			return "types.BytesArray", c.UDTName
		case "_bit", "_interval", "_varbit", "_char", "_money", "_varchar", "_cidr", "_inet", "_macaddr", "_citext", "_text", "_uuid", "_xml":
			return "types.StringArray", c.UDTName
		case "_bool":
			return "types.BoolArray", c.UDTName
		case "_numeric":
			return "types.DecimalArray", c.UDTName
		case "_float4", "_float8":
			return "types.Float64Array", c.UDTName
		default:
			return "types.StringArray", c.UDTName
		}
	}
}

// Imports for the postgres driver
func (p PostgresDriver) Imports() (importers.Collection, error) {
	var col importers.Collection

	col.All = importers.Set{
		Standard: importers.List{
			`"strconv"`,
		},
	}
	col.Singleton = importers.Map{
		"psql_upsert": {
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
		"psql_suites_test": {
			Standard: importers.List{
				`"testing"`,
			},
		},
		"psql_main_test": {
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
				`"github.com/aarondl/sqlboiler/v4/drivers/sqlboiler-psql/driver"`,
				`"github.com/aarondl/randomize"`,
				`_ "github.com/lib/pq"`,
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
		"null.JSON": {
			ThirdParty: importers.List{`"github.com/aarondl/null/v8"`},
		},
		"null.Bytes": {
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
		"types.Byte": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.BytesArray": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.Int64Array": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.Float64Array": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.BoolArray": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.StringArray": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.DecimalArray": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"types.HStore": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"pgeo.Point": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Line": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Lseg": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Box": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Path": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.Polygon": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"types.NullDecimal": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types"`},
		},
		"pgeo.Circle": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullPoint": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullLine": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullLseg": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullBox": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullPath": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullPolygon": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
		"pgeo.NullCircle": {
			ThirdParty: importers.List{`"github.com/aarondl/sqlboiler/v4/types/pgeo"`},
		},
	}

	return col, nil
}

// getVersion gets the version of underlying database
func (p *PostgresDriver) getVersion() (int, error) {
	type versionInfoType struct {
		ServerVersionNum int `json:"server_version_num"`
	}
	versionInfo := &versionInfoType{}

	row := p.conn.QueryRow("SHOW server_version_num")
	if err := row.Scan(&versionInfo.ServerVersionNum); err != nil {
		return 0, err
	}

	return versionInfo.ServerVersionNum, nil
}
