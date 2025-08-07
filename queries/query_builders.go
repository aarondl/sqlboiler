package queries

import (
	"bytes"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/aarondl/strmangle"
)

var (
	rgxIdentifier  = regexp.MustCompile(`^(?i)"?[a-z_][_a-z0-9]*"?(?:\."?[_a-z][_a-z0-9]*"?)*$`)
	rgxInClause    = regexp.MustCompile(`^(?i)(.*[\s|\)|\?])IN([\s|\(|\?].*)$`)
	rgxNotInClause = regexp.MustCompile(`^(?i)(.*[\s|\)|\?])NOT\s+IN([\s|\(|\?].*)$`)
)

// BuildQuery builds a query object into the query string
// and it's accompanying arguments. Using this method
// allows query building without immediate execution.
func BuildQuery(q *Query) (string, []interface{}) {
	var buf *bytes.Buffer
	var args []interface{}

	q.removeSoftDeleteWhere()

	switch {
	case len(q.rawSQL.sql) != 0:
		return q.rawSQL.sql, q.rawSQL.args
	case q.delete:
		buf, args = buildDeleteQuery(q)
	case len(q.update) > 0:
		buf, args = buildUpdateQuery(q)
	default:
		buf, args = buildSelectQuery(q)
	}

	defer strmangle.PutBuffer(buf)

	// Cache the generated query for query object re-use
	bufStr := buf.String()
	q.rawSQL.sql = bufStr
	q.rawSQL.args = args

	return bufStr, args
}

func buildSelectQuery(q *Query) (*bytes.Buffer, []interface{}) {
	buf := strmangle.GetBuffer()
	var args []interface{}

	writeComment(q, buf)
	writeCTEs(q, buf, &args)

	hasHaving := len(q.having) != 0
	hasGroupBy := len(q.groupBy) != 0
	hasSimpleCount := q.count && !hasHaving && !hasGroupBy
	hasComplexCount := q.count && (hasHaving || hasGroupBy)
	if hasComplexCount {
		buf.WriteString("SELECT COUNT(*) FROM (")
	}

	buf.WriteString("SELECT ")

	if q.dialect.UseTopClause {
		if q.limit != nil && q.offset == 0 {
			fmt.Fprintf(buf, " TOP (%d) ", *q.limit)
		}
	}

	if hasSimpleCount {
		buf.WriteString("COUNT(")
	}

	hasSelectCols := len(q.selectCols) != 0
	hasJoins := len(q.joins) != 0
	hasDistinct := q.distinct != ""
	if hasDistinct {
		buf.WriteString("DISTINCT ")
		if hasSimpleCount {
			buf.WriteString("(")
		}
		buf.WriteString(q.distinct)
		if hasSimpleCount {
			buf.WriteString(")")
		}
	} else if hasJoins && hasSelectCols && !hasSimpleCount {
		selectColsWithAs := writeAsStatements(q)
		// Don't identQuoteSlice - writeAsStatements does this
		buf.WriteString(strings.Join(selectColsWithAs, ", "))
	} else if hasSelectCols {
		buf.WriteString(strings.Join(strmangle.IdentQuoteSlice(q.dialect.LQ, q.dialect.RQ, q.selectCols), ", "))
	} else if hasJoins && !hasSimpleCount {
		selectColsWithStars := writeStars(q)
		buf.WriteString(strings.Join(selectColsWithStars, ", "))
	} else {
		buf.WriteByte('*')
	}

	// close SQL COUNT function
	if hasSimpleCount {
		buf.WriteByte(')')
	}

	fmt.Fprintf(buf, " FROM %s", strings.Join(strmangle.IdentQuoteSlice(q.dialect.LQ, q.dialect.RQ, q.from), ", "))

	if len(q.joins) > 0 {
		argsLen := len(args)
		joinBuf := strmangle.GetBuffer()
		for _, j := range q.joins {
			switch j.kind {
			case JoinInner:
				fmt.Fprintf(joinBuf, " INNER JOIN %s", j.clause)
			case JoinOuterLeft:
				fmt.Fprintf(joinBuf, " LEFT JOIN %s", j.clause)
			case JoinOuterRight:
				fmt.Fprintf(joinBuf, " RIGHT JOIN %s", j.clause)
			case JoinOuterFull:
				fmt.Fprintf(joinBuf, " FULL JOIN %s", j.clause)
			default:
				panic(fmt.Sprintf("Unsupported join of kind %v", j.kind))
			}
			args = append(args, j.args...)
		}
		var resp string
		if q.dialect.UseIndexPlaceholders {
			resp, _ = convertQuestionMarks(joinBuf.String(), argsLen+1)
		} else {
			resp = joinBuf.String()
		}
		buf.WriteString(resp)
		strmangle.PutBuffer(joinBuf)
	}

	where, whereArgs := whereClause(q, len(args)+1)
	buf.WriteString(where)
	if len(whereArgs) != 0 {
		args = append(args, whereArgs...)
	}

	writeModifiers(q, buf, &args)

	if hasComplexCount {
		buf.WriteString(") AS q")
	}
	buf.WriteByte(';')
	return buf, args
}

func buildDeleteQuery(q *Query) (*bytes.Buffer, []interface{}) {
	var args []interface{}
	buf := strmangle.GetBuffer()

	writeComment(q, buf)
	writeCTEs(q, buf, &args)

	buf.WriteString("DELETE FROM ")
	buf.WriteString(strings.Join(strmangle.IdentQuoteSlice(q.dialect.LQ, q.dialect.RQ, q.from), ", "))

	where, whereArgs := whereClause(q, 1)
	if len(whereArgs) != 0 {
		args = append(args, whereArgs...)
	}
	buf.WriteString(where)

	writeModifiers(q, buf, &args)

	buf.WriteByte(';')

	return buf, args
}

func buildUpdateQuery(q *Query) (*bytes.Buffer, []interface{}) {
	buf := strmangle.GetBuffer()
	var args []interface{}

	writeComment(q, buf)
	writeCTEs(q, buf, &args)

	buf.WriteString("UPDATE ")
	buf.WriteString(strings.Join(strmangle.IdentQuoteSlice(q.dialect.LQ, q.dialect.RQ, q.from), ", "))

	cols := make(sort.StringSlice, len(q.update))

	count := 0
	for name := range q.update {
		cols[count] = name
		count++
	}

	cols.Sort()

	for i := 0; i < len(cols); i++ {
		args = append(args, q.update[cols[i]])
		cols[i] = strmangle.IdentQuote(q.dialect.LQ, q.dialect.RQ, cols[i])
	}

	setSlice := make([]string, len(cols))
	for index, col := range cols {
		setSlice[index] = fmt.Sprintf("%s = %s", col, strmangle.Placeholders(q.dialect.UseIndexPlaceholders, 1, index+1, 1))
	}
	fmt.Fprintf(buf, " SET %s", strings.Join(setSlice, ", "))

	where, whereArgs := whereClause(q, len(args)+1)
	if len(whereArgs) != 0 {
		args = append(args, whereArgs...)
	}
	buf.WriteString(where)

	writeModifiers(q, buf, &args)

	buf.WriteByte(';')

	return buf, args
}

func writeParameterizedModifiers(q *Query, buf *bytes.Buffer, args *[]interface{}, keyword, delim string, clauses []argClause) {
	argsLen := len(*args)
	modBuf := strmangle.GetBuffer()
	modBuf.WriteString(keyword)

	for i, j := range clauses {
		if i > 0 {
			modBuf.WriteString(delim)
		}
		modBuf.WriteString(j.clause)
		*args = append(*args, j.args...)
	}

	var resp string
	if q.dialect.UseIndexPlaceholders {
		resp, _ = convertQuestionMarks(modBuf.String(), argsLen+1)
	} else {
		resp = modBuf.String()
	}

	buf.WriteString(resp)
	strmangle.PutBuffer(modBuf)
}

func writeModifiers(q *Query, buf *bytes.Buffer, args *[]interface{}) {
	if len(q.groupBy) != 0 {
		fmt.Fprintf(buf, " GROUP BY %s", strings.Join(q.groupBy, ", "))
	}

	if len(q.having) != 0 {
		writeParameterizedModifiers(q, buf, args, " HAVING ", " AND ", q.having)
	}

	if len(q.orderBy) != 0 {
		writeParameterizedModifiers(q, buf, args, " ORDER BY ", ", ", q.orderBy)
	}

	if !q.dialect.UseTopClause {
		if q.limit != nil {
			fmt.Fprintf(buf, " LIMIT %d", *q.limit)
		}

		if q.offset != 0 {
			fmt.Fprintf(buf, " OFFSET %d", q.offset)
		}
	} else {
		// From MS SQL 2012 and above: https://technet.microsoft.com/en-us/library/ms188385(v=sql.110).aspx
		// ORDER BY ...
		// OFFSET N ROWS
		// FETCH NEXT M ROWS ONLY
		if q.offset != 0 {

			// Hack from https://www.microsoftpressstore.com/articles/article.aspx?p=2314819
			// ...
			// As mentioned, the OFFSET-FETCH filter requires an ORDER BY clause. If you want to use arbitrary order,
			// like TOP without an ORDER BY clause, you can use the trick with ORDER BY (SELECT NULL)
			// ...
			if len(q.orderBy) == 0 {
				buf.WriteString(" ORDER BY (SELECT NULL)")
			}

			// This seems to be the latest version of mssql's syntax for offset
			// (the suffix ROWS)
			// This is true for latest sql server as well as their cloud offerings & the upcoming sql server 2019
			// https://docs.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql?view=sql-server-2017
			// https://docs.microsoft.com/en-us/sql/t-sql/queries/select-order-by-clause-transact-sql?view=sql-server-ver15
			fmt.Fprintf(buf, " OFFSET %d ROWS", q.offset)

			if q.limit != nil {
				fmt.Fprintf(buf, " FETCH NEXT %d ROWS ONLY", *q.limit)
			}
		}
	}

	if len(q.forlock) != 0 {
		fmt.Fprintf(buf, " FOR %s", q.forlock)
	}
}

func writeStars(q *Query) []string {
	cols := make([]string, len(q.from))
	for i, f := range q.from {
		toks := strings.Split(f, " ")
		if len(toks) == 1 {
			cols[i] = fmt.Sprintf(`%s.*`, strmangle.IdentQuote(q.dialect.LQ, q.dialect.RQ, toks[0]))
			continue
		}

		alias, name, ok := parseFromClause(toks)
		if !ok {
			return nil
		}

		if len(alias) != 0 {
			name = alias
		}
		cols[i] = fmt.Sprintf(`%s.*`, strmangle.IdentQuote(q.dialect.LQ, q.dialect.RQ, name))
	}

	return cols
}

func writeAsStatements(q *Query) []string {
	cols := make([]string, len(q.selectCols))
	for i, col := range q.selectCols {
		if !rgxIdentifier.MatchString(col) {
			cols[i] = col
			continue
		}

		toks := strings.Split(col, ".")
		if len(toks) == 1 {
			cols[i] = strmangle.IdentQuote(q.dialect.LQ, q.dialect.RQ, col)
			continue
		}

		asParts := make([]string, len(toks))
		for j, tok := range toks {
			asParts[j] = strings.Trim(tok, `"`)
		}

		cols[i] = fmt.Sprintf(`%s as "%s"`, strmangle.IdentQuote(q.dialect.LQ, q.dialect.RQ, col), strings.Join(asParts, "."))
	}

	return cols
}

// whereClause parses a where slice and converts it into a
// single WHERE clause like:
// WHERE (a=$1) AND (b=$2) AND (a,b) in (($3, $4), ($5, $6))
//
// startAt specifies what number placeholders start at
func whereClause(q *Query, startAt int) (string, []interface{}) {
	if len(q.where) == 0 {
		return "", nil
	}

	manualParens := false
ManualParen:
	for _, w := range q.where {
		switch w.kind {
		case whereKindLeftParen, whereKindRightParen:
			manualParens = true
			break ManualParen
		}
	}

	buf := strmangle.GetBuffer()
	defer strmangle.PutBuffer(buf)
	var args []interface{}

	notFirstExpression := false
	buf.WriteString(" WHERE ")
	for _, where := range q.where {
		if notFirstExpression && where.kind != whereKindRightParen {
			if where.orSeparator {
				buf.WriteString(" OR ")
			} else {
				buf.WriteString(" AND ")
			}
		} else {
			notFirstExpression = true
		}

		switch where.kind {
		case whereKindNormal:
			if !manualParens {
				buf.WriteByte('(')
			}
			if q.dialect.UseIndexPlaceholders {
				replaced, n := convertQuestionMarks(where.clause, startAt)
				buf.WriteString(replaced)
				startAt += n
			} else {
				buf.WriteString(where.clause)
			}
			if !manualParens {
				buf.WriteByte(')')
			}
			args = append(args, where.args...)
		case whereKindLeftParen:
			buf.WriteByte('(')
			notFirstExpression = false
		case whereKindRightParen:
			buf.WriteByte(')')
		case whereKindIn, whereKindNotIn:
			ln := len(where.args)
			// WHERE IN () is invalid sql, so it is difficult to simply run code like:
			// for _, u := range model.Users(qm.WhereIn("id IN ?",uids...)).AllP(db) {
			//    ...
			// }
			// instead when we see empty IN we produce 1=0 so it can still be chained
			// with other queries
			if ln == 0 {
				if where.kind == whereKindIn {
					buf.WriteString("(1=0)")
				} else if where.kind == whereKindNotIn {
					buf.WriteString("(1=1)")
				}
				break
			}

			var matches []string
			if where.kind == whereKindIn {
				matches = rgxInClause.FindStringSubmatch(where.clause)
			} else {
				matches = rgxNotInClause.FindStringSubmatch(where.clause)
			}

			// If we can't find any matches attempt a simple replace with 1 group.
			// Clauses that fit this criteria will not be able to contain ? in their
			// column name side, however if this case is being hit then the regexp
			// probably needs adjustment, or the user is passing in invalid clauses.
			if matches == nil {
				clause, count := convertInQuestionMarks(q.dialect.UseIndexPlaceholders, where.clause, startAt, 1, ln)
				if !manualParens {
					buf.WriteByte('(')
				}
				buf.WriteString(clause)
				if !manualParens {
					buf.WriteByte(')')
				}
				args = append(args, where.args...)
				startAt += count
				break
			}

			leftSide := strings.TrimSpace(matches[1])
			rightSide := strings.TrimSpace(matches[2])
			// If matches are found, we have to parse the left side (column side)
			// of the clause to determine how many columns they are using.
			// This number determines the groupAt for the convert function.
			cols := strings.Split(leftSide, ",")
			cols = strmangle.IdentQuoteSlice(q.dialect.LQ, q.dialect.RQ, cols)
			groupAt := len(cols)

			var leftClause string
			var leftCount int
			if q.dialect.UseIndexPlaceholders {
				leftClause, leftCount = convertQuestionMarks(strings.Join(cols, ","), startAt)
			} else {
				// Count the number of cols that are question marks, so we know
				// how much to offset convertInQuestionMarks by
				for _, v := range cols {
					if v == "?" {
						leftCount++
					}
				}
				leftClause = strings.Join(cols, ",")
			}
			rightClause, rightCount := convertInQuestionMarks(q.dialect.UseIndexPlaceholders, rightSide, startAt+leftCount, groupAt, ln-leftCount)
			if !manualParens {
				buf.WriteByte('(')
			}
			buf.WriteString(leftClause)
			if where.kind == whereKindIn {
				buf.WriteString(" IN ")
			} else if where.kind == whereKindNotIn {
				buf.WriteString(" NOT IN ")
			}
			buf.WriteString(rightClause)
			if !manualParens {
				buf.WriteByte(')')
			}
			startAt += leftCount + rightCount
			args = append(args, where.args...)
		default:
			panic("unknown where type")
		}
	}

	return buf.String(), args
}

// convertInQuestionMarks finds the first unescaped occurrence of ? and swaps it
// with a list of numbered placeholders, starting at startAt.
// It uses groupAt to determine how many placeholders should be in each group,
// for example, groupAt 2 would result in: (($1,$2),($3,$4))
// and groupAt 1 would result in ($1,$2,$3,$4)
func convertInQuestionMarks(UseIndexPlaceholders bool, clause string, startAt, groupAt, total int) (string, int) {
	if startAt == 0 || len(clause) == 0 {
		panic("Not a valid start number.")
	}

	paramBuf := strmangle.GetBuffer()
	defer strmangle.PutBuffer(paramBuf)

	foundAt := -1
	for i := 0; i < len(clause); i++ {
		if (i == 0 && clause[i] == '?') || (clause[i] == '?' && clause[i-1] != '\\') {
			foundAt = i
			break
		}
	}

	if foundAt == -1 {
		return strings.ReplaceAll(clause, `\?`, "?"), 0
	}

	paramBuf.WriteString(clause[:foundAt])
	paramBuf.WriteByte('(')
	paramBuf.WriteString(strmangle.Placeholders(UseIndexPlaceholders, total, startAt, groupAt))
	paramBuf.WriteByte(')')
	paramBuf.WriteString(clause[foundAt+1:])

	// Remove all backslashes from escaped question-marks
	ret := strings.ReplaceAll(paramBuf.String(), `\?`, "?")
	return ret, total
}

// convertQuestionMarks converts each occurrence of ? with $<number>
// where <number> is an incrementing digit starting at startAt.
// If question-mark (?) is escaped using back-slash (\), it will be ignored.
func convertQuestionMarks(clause string, startAt int) (string, int) {
	if startAt == 0 {
		panic("Not a valid start number.")
	}

	paramBuf := strmangle.GetBuffer()
	defer strmangle.PutBuffer(paramBuf)
	paramIndex := 0
	total := 0

	for {
		if paramIndex >= len(clause) {
			break
		}

		clause = clause[paramIndex:]
		paramIndex = strings.IndexByte(clause, '?')

		if paramIndex == -1 {
			paramBuf.WriteString(clause)
			break
		}

		escapeIndex := strings.Index(clause, `\?`)
		if escapeIndex != -1 && paramIndex > escapeIndex {
			paramBuf.WriteString(clause[:escapeIndex] + "?")
			paramIndex++
			continue
		}

		paramBuf.WriteString(clause[:paramIndex] + fmt.Sprintf("$%d", startAt))
		total++
		startAt++
		paramIndex++
	}

	return paramBuf.String(), total
}

// parseFromClause will parse something that looks like
// a
// a b
// a as b
func parseFromClause(toks []string) (alias, name string, ok bool) {
	if len(toks) > 3 {
		toks = toks[:3]
	}

	sawIdent, sawAs := false, false
	for _, tok := range toks {
		if t := strings.ToLower(tok); sawIdent && t == "as" {
			sawAs = true
			continue
		} else if sawIdent && t == "on" {
			break
		}

		if !rgxIdentifier.MatchString(tok) {
			break
		}

		if sawIdent || sawAs {
			alias = strings.Trim(tok, `"`)
			break
		}

		name = strings.Trim(tok, `"`)
		sawIdent = true
		ok = true
	}

	return alias, name, ok
}

var commnetSplit = regexp.MustCompile(`[\n\r]+`)

func writeComment(q *Query, buf *bytes.Buffer) {
	if len(q.comment) == 0 {
		return
	}

	lines := commnetSplit.Split(q.comment, -1)
	for _, line := range lines {
		buf.WriteString("-- ")
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
}

func writeCTEs(q *Query, buf *bytes.Buffer, args *[]interface{}) {
	if len(q.withs) == 0 {
		return
	}

	buf.WriteString("WITH")
	argsLen := len(*args)
	withBuf := strmangle.GetBuffer()
	lastPos := len(q.withs) - 1
	for i, w := range q.withs {
		fmt.Fprintf(withBuf, " %s", w.clause)
		if i >= 0 && i < lastPos {
			withBuf.WriteByte(',')
		}
		*args = append(*args, w.args...)
	}
	withBuf.WriteByte(' ')
	var resp string
	if q.dialect.UseIndexPlaceholders {
		resp, _ = convertQuestionMarks(withBuf.String(), argsLen+1)
	} else {
		resp = withBuf.String()
	}
	buf.WriteString(resp)
	strmangle.PutBuffer(withBuf)
}
