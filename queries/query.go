package queries

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"

	"github.com/aarondl/sqlboiler/v4/boil"
	"github.com/aarondl/sqlboiler/v4/drivers"
)

// joinKind is the type of join
type joinKind int

// Join type constants
const (
	JoinInner joinKind = iota
	JoinOuterLeft
	JoinOuterRight
	JoinNatural
	JoinOuterFull
)

// Query holds the state for the built up query
type Query struct {
	dialect *drivers.Dialect
	rawSQL  rawSQL

	load     []string
	loadMods map[string]Applicator

	delete        bool
	update        map[string]interface{}
	withs         []argClause
	selectCols    []string
	count         bool
	from          []string
	joins         []join
	where         []where
	groupBy       []string
	orderBy       []argClause
	having        []argClause
	limit         *int
	offset        int
	forlock       string
	distinct      string
	comment       string
	appendComment string

	// This field is a hack to allow a query to strip out the reference
	// to deleted at is null.
	removeSoftDelete bool
}

// Applicator exists only to allow
// query mods into the query struct around
// eager loaded relationships.
type Applicator interface {
	Apply(*Query)
}

type whereKind int

const (
	whereKindNormal whereKind = iota
	whereKindLeftParen
	whereKindRightParen
	whereKindIn
	whereKindNotIn
)

type where struct {
	kind whereKind

	clause      string
	orSeparator bool
	args        []interface{}
}

type in struct {
	clause      string
	orSeparator bool
	args        []interface{}
}

type argClause struct {
	clause string
	args   []interface{}
}

type rawSQL struct {
	sql  string
	args []interface{}
}

type join struct {
	kind   joinKind
	clause string
	args   []interface{}
}

// Raw makes a raw query, usually for use with bind
func Raw(query string, args ...interface{}) *Query {
	return &Query{
		rawSQL: rawSQL{
			sql:  query,
			args: args,
		},
	}
}

// RawG makes a raw query using the global boil.Executor, usually for use with bind
func RawG(query string, args ...interface{}) *Query {
	return Raw(query, args...)
}

// Exec executes a query that does not need a row returned
func (q *Query) Exec(exec boil.Executor) (sql.Result, error) {
	qs, args := BuildQuery(q)
	if boil.DebugMode {
		fmt.Fprintln(boil.DebugWriter, qs)
		fmt.Fprintln(boil.DebugWriter, args)
	}
	return exec.Exec(qs, args...)
}

// QueryRowG executes the query for the One finisher and returns a row.
// It uses the global executer.
func (q *Query) QueryRowG() *sql.Row {
	return q.QueryRow(boil.GetDB())
}

// QueryRow executes the query for the One finisher and returns a row
func (q *Query) QueryRow(exec boil.Executor) *sql.Row {
	qs, args := BuildQuery(q)
	if boil.DebugMode {
		fmt.Fprintln(boil.DebugWriter, qs)
		fmt.Fprintln(boil.DebugWriter, args)
	}
	return exec.QueryRow(qs, args...)
}

// Query executes the query for the All finisher and returns multiple rows
func (q *Query) Query(exec boil.Executor) (*sql.Rows, error) {
	qs, args := BuildQuery(q)
	if boil.DebugMode {
		fmt.Fprintln(boil.DebugWriter, qs)
		fmt.Fprintln(boil.DebugWriter, args)
	}
	return exec.Query(qs, args...)
}

// ExecContext executes a query that does not need a row returned
func (q *Query) ExecContext(ctx context.Context, exec boil.ContextExecutor) (sql.Result, error) {
	qs, args := BuildQuery(q)
	if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
		fmt.Fprintln(writer, qs)
		fmt.Fprintln(writer, args)
	}
	return exec.ExecContext(ctx, qs, args...)
}

// QueryRowContextG executes the query for the One finisher and returns a row.
// It uses the global executer.
func (q *Query) QueryRowContextG(ctx context.Context) *sql.Row {
	return q.QueryRowContext(ctx, boil.GetContextDB())
}

// QueryRowContext executes the query for the One finisher and returns a row
func (q *Query) QueryRowContext(ctx context.Context, exec boil.ContextExecutor) *sql.Row {
	qs, args := BuildQuery(q)
	if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
		fmt.Fprintln(writer, qs)
		fmt.Fprintln(writer, args)
	}
	return exec.QueryRowContext(ctx, qs, args...)
}

// QueryContext executes the query for the All finisher and returns multiple rows
func (q *Query) QueryContext(ctx context.Context, exec boil.ContextExecutor) (*sql.Rows, error) {
	qs, args := BuildQuery(q)
	if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
		fmt.Fprintln(writer, qs)
		fmt.Fprintln(writer, args)
	}
	return exec.QueryContext(ctx, qs, args...)
}

// ExecG executes a query that does not need a row returned.
// It uses the global executer.
func (q *Query) ExecG() (sql.Result, error) {
	return q.Exec(boil.GetDB())
}

// ExecP executes a query that does not need a row returned
// It will panic on error
func (q *Query) ExecP(exec boil.Executor) sql.Result {
	res, err := q.Exec(exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return res
}

// ExecGP executes a query that does not need a row returned.
// It uses the global executer and panics on error.
func (q *Query) ExecGP() sql.Result {
	return q.ExecP(boil.GetDB())
}

// QueryG executes the query for the All finisher and returns multiple rows.
// It uses the global executer.
func (q *Query) QueryG() (*sql.Rows, error) {
	return q.Query(boil.GetDB())
}

// QueryP executes the query for the All finisher and returns multiple rows
// It will panic on error
func (q *Query) QueryP(exec boil.Executor) *sql.Rows {
	rows, err := q.Query(exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return rows
}

// QueryGP executes the query for the All finisher and returns multiple rows.
// It uses the global executer and panics on error.
func (q *Query) QueryGP() *sql.Rows {
	return q.QueryP(boil.GetDB())
}

// ExecContextG executes a query that does not need a row returned with a given context.
// It uses the global executer.
func (q *Query) ExecContextG(ctx context.Context) (sql.Result, error) {
	return q.ExecContext(ctx, boil.GetContextDB())
}

// ExecContextP executes a query with a given context that does not need a row returned.
// It panics on error.
func (q *Query) ExecContextP(ctx context.Context, exec boil.ContextExecutor) sql.Result {
	result, err := q.ExecContext(ctx, exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return result
}

// ExecContextGP executes a query with a given context that does not need a row returned.
// It uses the global executer and panics on error.
func (q *Query) ExecContextGP(ctx context.Context) sql.Result {
	return q.ExecContextP(ctx, boil.GetContextDB())
}

// QueryContextG executes the query for the All finisher with a given context and returns multiple rows.
// It uses the global executer.
func (q *Query) QueryContextG(ctx context.Context) (*sql.Rows, error) {
	return q.QueryContext(ctx, boil.GetContextDB())
}

// QueryContextP executes the query for the All finisher with a given context and returns multiple rows.
// It panics on error.
func (q *Query) QueryContextP(ctx context.Context, exec boil.ContextExecutor) *sql.Rows {
	rows, err := q.QueryContext(ctx, exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return rows
}

// QueryContextGP executes the query for the All finisher with a given context and returns multiple rows.
// It uses the global executer and panics on error.
func (q *Query) QueryContextGP(ctx context.Context) *sql.Rows {
	return q.QueryContextP(ctx, boil.GetContextDB())
}

// SetDialect on the query.
func SetDialect(q *Query, dialect *drivers.Dialect) {
	q.dialect = dialect
}

// SetSQL on the query.
func SetSQL(q *Query, sql string, args ...interface{}) {
	q.rawSQL = rawSQL{sql: sql, args: args}
}

// SetArgs is primarily for re-use of a query so that the
// query text does not need to be re-generated, useful
// if you're performing the same query with different arguments
// over and over.
func SetArgs(q *Query, args ...interface{}) {
	q.rawSQL.args = args
}

// SetLoad on the query.
func SetLoad(q *Query, relationships ...string) {
	q.load = append([]string(nil), relationships...)
}

// AppendLoad on the query.
func AppendLoad(q *Query, relationships string) {
	q.load = append(q.load, relationships)
}

// SetLoadMods on the query.
func SetLoadMods(q *Query, rel string, appl Applicator) {
	if q.loadMods == nil {
		q.loadMods = make(map[string]Applicator)
	}

	q.loadMods[rel] = appl
}

// SetSelect on the query.
func SetSelect(q *Query, sel []string) {
	q.selectCols = sel
}

// GetSelect from the query
func GetSelect(q *Query) []string {
	return q.selectCols
}

// SetDistinct on the query.
func SetDistinct(q *Query, distinct string) {
	q.distinct = distinct
}

// SetCount on the query.
func SetCount(q *Query) {
	q.count = true
}

// SetDelete on the query.
func SetDelete(q *Query) {
	q.delete = true
}

// SetLimit on the query.
func SetLimit(q *Query, limit int) {
	q.limit = &limit
}

// SetOffset on the query.
func SetOffset(q *Query, offset int) {
	q.offset = offset
}

// SetFor on the query.
func SetFor(q *Query, clause string) {
	q.forlock = clause
}

// SetComment on the query.
func SetComment(q *Query, comment string) {
	q.comment = comment
}

// SetAppendComment on the query.
func SetAppendComment(q *Query, appendComment string) {
	q.appendComment = appendComment
}

// SetUpdate on the query.
func SetUpdate(q *Query, cols map[string]interface{}) {
	q.update = cols
}

// AppendSelect on the query.
func AppendSelect(q *Query, columns ...string) {
	q.selectCols = append(q.selectCols, columns...)
}

// AppendFrom on the query.
func AppendFrom(q *Query, from ...string) {
	q.from = append(q.from, from...)
}

// SetFrom replaces the current from statements.
func SetFrom(q *Query, from ...string) {
	q.from = append([]string(nil), from...)
}

// AppendInnerJoin on the query.
func AppendInnerJoin(q *Query, clause string, args ...interface{}) {
	q.joins = append(q.joins, join{clause: clause, kind: JoinInner, args: args})
}

// AppendLeftOuterJoin on the query.
func AppendLeftOuterJoin(q *Query, clause string, args ...interface{}) {
	q.joins = append(q.joins, join{clause: clause, kind: JoinOuterLeft, args: args})
}

// AppendRightOuterJoin on the query.
func AppendRightOuterJoin(q *Query, clause string, args ...interface{}) {
	q.joins = append(q.joins, join{clause: clause, kind: JoinOuterRight, args: args})
}

// AppendFullOuterJoin on the query.
func AppendFullOuterJoin(q *Query, clause string, args ...interface{}) {
	q.joins = append(q.joins, join{clause: clause, kind: JoinOuterFull, args: args})
}

// AppendHaving on the query.
func AppendHaving(q *Query, clause string, args ...interface{}) {
	q.having = append(q.having, argClause{clause: clause, args: args})
}

// AppendWhere on the query.
func AppendWhere(q *Query, clause string, args ...interface{}) {
	q.where = append(q.where, where{clause: clause, args: args})
}

// AppendIn on the query.
func AppendIn(q *Query, clause string, args ...interface{}) {
	q.where = append(q.where, where{kind: whereKindIn, clause: clause, args: args})
}

// AppendNotIn on the query.
func AppendNotIn(q *Query, clause string, args ...interface{}) {
	q.where = append(q.where, where{kind: whereKindNotIn, clause: clause, args: args})
}

// SetLastWhereAsOr sets the or separator for the tail "WHERE" in the slice
func SetLastWhereAsOr(q *Query) {
	if len(q.where) == 0 {
		return
	}

	pos := len(q.where) - 1
	where := q.where[pos]
	if where.kind != whereKindRightParen {
		q.where[len(q.where)-1].orSeparator = true
		return
	}

	stack := 0
	pos--
	for ; pos >= 0; pos-- {
		switch q.where[pos].kind {
		case whereKindLeftParen:
			if stack == 0 {
				q.where[pos].orSeparator = true
				return
			}
			stack--
		case whereKindRightParen:
			stack++
		}
	}

	panic("could not find matching ( in where query expr")
}

// SetLastInAsOr is an alias for SetLastWhereAsOr
func SetLastInAsOr(q *Query) {
	SetLastWhereAsOr(q)
}

// AppendWhereLeftParen creates a right left in the where expression
func AppendWhereLeftParen(q *Query) {
	q.where = append(q.where, where{kind: whereKindLeftParen})
}

// AppendWhereRightParen creates a right paren in the where expression
func AppendWhereRightParen(q *Query) {
	q.where = append(q.where, where{kind: whereKindRightParen})
}

// AppendGroupBy on the query.
func AppendGroupBy(q *Query, clause string) {
	q.groupBy = append(q.groupBy, clause)
}

// AppendOrderBy on the query.
func AppendOrderBy(q *Query, clause string, args ...interface{}) {
	q.orderBy = append(q.orderBy, argClause{clause: clause, args: args})
}

// AppendWith on the query.
func AppendWith(q *Query, clause string, args ...interface{}) {
	q.withs = append(q.withs, argClause{clause: clause, args: args})
}

// RemoveSoftDeleteWhere prevents the automatic soft delete where clause
// from being included when building the query.
func RemoveSoftDeleteWhere(q *Query) {
	q.removeSoftDelete = true
}

var removeSoftDeleteRgx = regexp.MustCompile("deleted_at[\"'`]? is null")

// SetRemoveSoftDeleteRgx sets the removeSoftDeleteRgx variable to support
// qm.Where method to work with a custom soft delete auto-column name.
func SetRemoveSoftDeleteRgx(rgx *regexp.Regexp) {
	removeSoftDeleteRgx = rgx
}

// removeSoftDeleteWhere attempts to remove the soft delete where clause
// added automatically by sqlboiler.
func (q *Query) removeSoftDeleteWhere() {
	if !q.removeSoftDelete {
		return
	}

	for i := len(q.where) - 1; i >= 0; i-- {
		w := q.where[i]
		if w.kind != whereKindNormal || !removeSoftDeleteRgx.MatchString(w.clause) {
			continue
		}

		// It's of vital importance we preserve order here
		if i != len(q.where)-1 {
			// If this is not the last element we shift all elements
			// left one.
			copy(q.where[i:], q.where[i+1:])
		}
		q.where = q.where[:len(q.where)-1]
		// Only delete one of these - the rest could be from the user.
		break
	}
}
