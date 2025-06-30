package queries

import (
	"reflect"
	"testing"
)

func TestSetLimit(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetLimit(q, 10)

	expect := 10
	if q.limit == nil {
		t.Errorf("Expected %d, got nil", expect)
	} else if *q.limit != expect {
		t.Errorf("Expected %d, got %d", expect, *q.limit)
	}
}

func TestSetOffset(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetOffset(q, 10)

	expect := 10
	if q.offset != expect {
		t.Errorf("Expected %d, got %d", expect, q.offset)
	}
}

func TestSetSQL(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetSQL(q, "select * from thing", 5, 3)

	if len(q.rawSQL.args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.rawSQL.args))
	}

	if q.rawSQL.sql != "select * from thing" {
		t.Errorf("Was not expected string, got %s", q.rawSQL.sql)
	}
}

func TestSetLoad(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetLoad(q, "one", "two")

	if len(q.load) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.load))
	}

	if q.load[0] != "one" || q.load[1] != "two" {
		t.Errorf("Was not expected string, got %v", q.load)
	}
}

type apple struct{}

func (apple) Apply(*Query) {}

func TestSetLoadMods(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetLoadMods(q, "a", apple{})
	SetLoadMods(q, "b", apple{})

	if len(q.loadMods) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.loadMods))
	}
}

func TestAppendWhere(t *testing.T) {
	t.Parallel()

	q := &Query{}
	expect := "x > $1 AND y > $2"
	AppendWhere(q, expect, 5, 3)
	AppendWhere(q, expect, 5, 3)

	if len(q.where) != 2 {
		t.Errorf("%#v", q.where)
	}

	if q.where[0].clause != expect || q.where[1].clause != expect {
		t.Errorf("Expected %s, got %#v", expect, q.where)
	}

	if len(q.where[0].args) != 2 || len(q.where[0].args) != 2 {
		t.Errorf("arg length wrong: %#v", q.where)
	}

	if q.where[0].args[0].(int) != 5 || q.where[0].args[1].(int) != 3 {
		t.Errorf("args wrong: %#v", q.where)
	}

	q.where = []where{{clause: expect, args: []interface{}{5, 3}}}
	if q.where[0].clause != expect {
		t.Errorf("Expected %s, got %v", expect, q.where)
	}

	if len(q.where[0].args) != 2 {
		t.Errorf("Expected %d args, got %d", 2, len(q.where[0].args))
	}

	if q.where[0].args[0].(int) != 5 || q.where[0].args[1].(int) != 3 {
		t.Errorf("Args not set correctly, expected 5 & 3, got: %#v", q.where[0].args)
	}

	if len(q.where) != 1 {
		t.Errorf("%#v", q.where)
	}
}

func TestSetLastWhereAsOr(t *testing.T) {
	t.Parallel()
	q := &Query{}

	AppendWhere(q, "")

	if q.where[0].orSeparator {
		t.Errorf("Do not want or separator")
	}

	SetLastWhereAsOr(q)

	if len(q.where) != 1 {
		t.Errorf("Want len 1")
	}
	if !q.where[0].orSeparator {
		t.Errorf("Want or separator")
	}

	AppendWhere(q, "")
	SetLastWhereAsOr(q)

	if len(q.where) != 2 {
		t.Errorf("Want len 2")
	}
	if q.where[0].orSeparator != true {
		t.Errorf("Expected true")
	}
	if q.where[1].orSeparator != true {
		t.Errorf("Expected true")
	}
}

func TestAppendIn(t *testing.T) {
	t.Parallel()

	q := &Query{}
	expect := "col IN ?"
	AppendIn(q, expect, 5, 3)
	AppendIn(q, expect, 5, 3)

	if len(q.where) != 2 {
		t.Errorf("%#v", q.where)
	}

	if q.where[0].clause != expect || q.where[1].clause != expect {
		t.Errorf("Expected %s, got %#v", expect, q.where)
	}

	if len(q.where[0].args) != 2 || len(q.where[0].args) != 2 {
		t.Errorf("arg length wrong: %#v", q.where)
	}

	if q.where[0].args[0].(int) != 5 || q.where[0].args[1].(int) != 3 {
		t.Errorf("args wrong: %#v", q.where)
	}

	q.where = []where{{clause: expect, args: []interface{}{5, 3}}}
	if q.where[0].clause != expect {
		t.Errorf("Expected %s, got %v", expect, q.where)
	}

	if len(q.where[0].args) != 2 {
		t.Errorf("Expected %d args, got %d", 2, len(q.where[0].args))
	}

	if q.where[0].args[0].(int) != 5 || q.where[0].args[1].(int) != 3 {
		t.Errorf("Args not set correctly, expected 5 & 3, got: %#v", q.where[0].args)
	}

	if len(q.where) != 1 {
		t.Errorf("%#v", q.where)
	}
}

func TestSetLastInAsOr(t *testing.T) {
	t.Parallel()
	q := &Query{}

	AppendIn(q, "")

	if q.where[0].orSeparator {
		t.Errorf("Do not want or separator")
	}

	SetLastInAsOr(q)

	if len(q.where) != 1 {
		t.Errorf("Want len 1")
	}
	if !q.where[0].orSeparator {
		t.Errorf("Want or separator")
	}

	AppendIn(q, "")
	SetLastInAsOr(q)

	if len(q.where) != 2 {
		t.Errorf("Want len 2")
	}
	if q.where[0].orSeparator != true {
		t.Errorf("Expected true")
	}
	if q.where[1].orSeparator != true {
		t.Errorf("Expected true")
	}
}

func TestAppendGroupBy(t *testing.T) {
	t.Parallel()

	q := &Query{}
	expect := "col1, col2"
	AppendGroupBy(q, expect)
	AppendGroupBy(q, expect)

	if len(q.groupBy) != 2 && (q.groupBy[0] != expect || q.groupBy[1] != expect) {
		t.Errorf("Expected %s, got %s %s", expect, q.groupBy[0], q.groupBy[1])
	}

	q.groupBy = []string{expect}
	if len(q.groupBy) != 1 && q.groupBy[0] != expect {
		t.Errorf("Expected %s, got %s", expect, q.groupBy[0])
	}
}

func TestAppendOrderBy(t *testing.T) {
	t.Parallel()

	q := &Query{}
	expect := "col1 desc, col2 asc"
	AppendOrderBy(q, expect, 10)
	AppendOrderBy(q, expect, 10)

	if len(q.orderBy) != 2 && (q.orderBy[0].clause != expect || q.orderBy[1].clause != expect) {
		t.Errorf("Expected %s, got %s %s", expect, q.orderBy[0], q.orderBy[1])
	}

	if q.orderBy[0].args[0] != 10 || q.orderBy[1].args[0] != 10 {
		t.Errorf("Expected %v, got %v %v", 10, q.orderBy[0].args[0], q.orderBy[1].args[0])
	}

	q.orderBy = []argClause{
		{"col1 desc, col2 asc", []interface{}{}},
	}
	if len(q.orderBy) != 1 && q.orderBy[0].clause != expect {
		t.Errorf("Expected %s, got %s", expect, q.orderBy[0].clause)
	}
}

func TestAppendHaving(t *testing.T) {
	t.Parallel()

	q := &Query{}
	expect := "count(orders.order_id) > ?"
	AppendHaving(q, expect, 10)
	AppendHaving(q, expect, 10)

	if len(q.having) != 2 {
		t.Errorf("Expected 2, got %d", len(q.having))
	}

	if q.having[0].clause != expect || q.having[1].clause != expect {
		t.Errorf("Expected %s, got %s %s", expect, q.having[0].clause, q.having[1].clause)
	}

	if q.having[0].args[0] != 10 || q.having[1].args[0] != 10 {
		t.Errorf("Expected %v, got %v %v", 10, q.having[0].args[0], q.having[1].args[0])
	}

	q.having = []argClause{{clause: expect, args: []interface{}{10}}}
	if len(q.having) != 1 && (q.having[0].clause != expect || q.having[0].args[0] != 10) {
		t.Errorf("Expected %s, got %s %v", expect, q.having[0], q.having[0].args[0])
	}
}

func TestFrom(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendFrom(q, "videos a", "orders b")
	AppendFrom(q, "videos a", "orders b")

	expect := []string{"videos a", "orders b", "videos a", "orders b"}
	if !reflect.DeepEqual(q.from, expect) {
		t.Errorf("Expected %s, got %s", expect, q.from)
	}

	SetFrom(q, "videos a", "orders b")
	if !reflect.DeepEqual(q.from, expect[:2]) {
		t.Errorf("Expected %s, got %s", expect, q.from)
	}
}

func TestSetSelect(t *testing.T) {
	t.Parallel()

	q := &Query{selectCols: []string{"hello"}}
	SetSelect(q, nil)

	if q.selectCols != nil {
		t.Errorf("want nil")
	}
}

func TestSetCount(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetCount(q)

	if q.count != true {
		t.Errorf("got false")
	}
}

func TestSetDistinct(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetDistinct(q, "id")

	if q.distinct != "id" {
		t.Errorf("expected id, got %v", q.distinct)
	}
}

func TestSetUpdate(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetUpdate(q, map[string]interface{}{"test": 5})

	if q.update["test"] != 5 {
		t.Errorf("Wrong update, got %v", q.update)
	}
}

func TestSetDelete(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetDelete(q)

	if q.delete != true {
		t.Errorf("Expected %t, got %t", true, q.delete)
	}
}

func TestSetArgs(t *testing.T) {
	t.Parallel()

	args := []interface{}{2}
	q := &Query{rawSQL: rawSQL{}}
	SetArgs(q, args...)

	if q.rawSQL.args[0].(int) != 2 {
		t.Errorf("Expected args to get set")
	}
}

func TestAppendSelect(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendSelect(q, "col1", "col2")
	AppendSelect(q, "col1", "col2")

	if len(q.selectCols) != 4 {
		t.Errorf("Expected selectCols len 4, got %d", len(q.selectCols))
	}

	if q.selectCols[0] != `col1` && q.selectCols[1] != `col2` {
		t.Errorf("select cols value mismatch: %#v", q.selectCols)
	}
	if q.selectCols[2] != `col1` && q.selectCols[3] != `col2` {
		t.Errorf("select cols value mismatch: %#v", q.selectCols)
	}

	q.selectCols = []string{"col1", "col2"}
	if q.selectCols[0] != `col1` && q.selectCols[1] != `col2` {
		t.Errorf("select cols value mismatch: %#v", q.selectCols)
	}
}

func TestSQL(t *testing.T) {
	t.Parallel()

	q := Raw("thing", 5)
	if q.rawSQL.sql != "thing" {
		t.Errorf("Expected %q, got %s", "thing", q.rawSQL.sql)
	}
	if q.rawSQL.args[0].(int) != 5 {
		t.Errorf("Expected 5, got %v", q.rawSQL.args[0])
	}
}

func TestSQLG(t *testing.T) {
	t.Parallel()

	q := RawG("thing", 5)
	if q.rawSQL.sql != "thing" {
		t.Errorf("Expected %q, got %s", "thing", q.rawSQL.sql)
	}
	if q.rawSQL.args[0].(int) != 5 {
		t.Errorf("Expected 5, got %v", q.rawSQL.args[0])
	}
}

func TestAppendInnerJoin(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendInnerJoin(q, "thing=$1 AND stuff=$2", 2, 5)
	AppendInnerJoin(q, "thing=$1 AND stuff=$2", 2, 5)

	if len(q.joins) != 2 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid innerJoin on string: %#v", q.joins)
	}
	if q.joins[1].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid innerJoin on string: %#v", q.joins)
	}

	if len(q.joins[0].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[0].args))
	}
	if len(q.joins[1].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[1].args))
	}

	if q.joins[0].args[0] != 2 && q.joins[0].args[1] != 5 {
		t.Errorf("Invalid args values, got %#v", q.joins[0].args)
	}

	q.joins = []join{{kind: JoinInner,
		clause: "thing=$1 AND stuff=$2",
		args:   []interface{}{2, 5},
	}}

	if len(q.joins) != 1 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid innerJoin on string: %#v", q.joins)
	}
}

func TestAppendLeftOuterJoin(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendLeftOuterJoin(q, "thing=$1 AND stuff=$2", 2, 5)
	AppendLeftOuterJoin(q, "thing=$1 AND stuff=$2", 2, 5)

	if len(q.joins) != 2 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid leftJoin on string: %#v", q.joins)
	}
	if q.joins[1].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid leftJoin on string: %#v", q.joins)
	}

	if len(q.joins[0].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[0].args))
	}
	if len(q.joins[1].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[1].args))
	}

	if q.joins[0].args[0] != 2 && q.joins[0].args[1] != 5 {
		t.Errorf("Invalid args values, got %#v", q.joins[0].args)
	}

	q.joins = []join{{kind: JoinOuterLeft,
		clause: "thing=$1 AND stuff=$2",
		args:   []interface{}{2, 5},
	}}

	if len(q.joins) != 1 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid leftJoin on string: %#v", q.joins)
	}
}

func TestAppendRightOuterJoin(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendRightOuterJoin(q, "thing=$1 AND stuff=$2", 2, 5)
	AppendRightOuterJoin(q, "thing=$1 AND stuff=$2", 2, 5)

	if len(q.joins) != 2 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid rightJoin on string: %#v", q.joins)
	}
	if q.joins[1].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid rightJoin on string: %#v", q.joins)
	}

	if len(q.joins[0].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[0].args))
	}
	if len(q.joins[1].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[1].args))
	}

	if q.joins[0].args[0] != 2 && q.joins[0].args[1] != 5 {
		t.Errorf("Invalid args values, got %#v", q.joins[0].args)
	}

	q.joins = []join{{kind: JoinOuterRight,
		clause: "thing=$1 AND stuff=$2",
		args:   []interface{}{2, 5},
	}}

	if len(q.joins) != 1 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid rightJoin on string: %#v", q.joins)
	}
}

func TestAppendFullOuterJoin(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendFullOuterJoin(q, "thing=$1 AND stuff=$2", 2, 5)
	AppendFullOuterJoin(q, "thing=$1 AND stuff=$2", 2, 5)

	if len(q.joins) != 2 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid fullJoin on string: %#v", q.joins)
	}
	if q.joins[1].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid fullJoin on string: %#v", q.joins)
	}

	if len(q.joins[0].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[0].args))
	}
	if len(q.joins[1].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.joins[1].args))
	}

	if q.joins[0].args[0] != 2 && q.joins[0].args[1] != 5 {
		t.Errorf("Invalid args values, got %#v", q.joins[0].args)
	}

	q.joins = []join{{kind: JoinOuterFull,
		clause: "thing=$1 AND stuff=$2",
		args:   []interface{}{2, 5},
	}}

	if len(q.joins) != 1 {
		t.Errorf("Expected len 1, got %d", len(q.joins))
	}

	if q.joins[0].clause != "thing=$1 AND stuff=$2" {
		t.Errorf("Got invalid fullJoin on string: %#v", q.joins)
	}
}

func TestAppendWith(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendWith(q, "cte_0 AS (SELECT * FROM table_0 WHERE thing=$1 AND stuff=$2)", 5, 10)
	AppendWith(q, "cte_1 AS (SELECT * FROM table_1 WHERE thing=$1 AND stuff=$2)", 5, 10)

	if len(q.withs) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.withs))
	}

	if q.withs[0].clause != "cte_0 AS (SELECT * FROM table_0 WHERE thing=$1 AND stuff=$2)" {
		t.Errorf("Got invalid with on string: %#v", q.withs)
	}
	if q.withs[1].clause != "cte_1 AS (SELECT * FROM table_1 WHERE thing=$1 AND stuff=$2)" {
		t.Errorf("Got invalid with on string: %#v", q.withs)
	}

	if len(q.withs[0].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.withs[0].args))
	}
	if len(q.withs[1].args) != 2 {
		t.Errorf("Expected len 2, got %d", len(q.withs[1].args))
	}

	if q.withs[0].args[0] != 5 && q.withs[0].args[1] != 10 {
		t.Errorf("Invalid args values, got %#v", q.withs[0].args)
	}

	q.withs = []argClause{{
		clause: "other_cte AS (SELECT * FROM other_table WHERE thing=$1 AND stuff=$2)",
		args:   []interface{}{3, 7},
	}}

	if len(q.withs) != 1 {
		t.Errorf("Expected len 1, got %d", len(q.withs))
	}

	if q.withs[0].clause != "other_cte AS (SELECT * FROM other_table WHERE thing=$1 AND stuff=$2)" {
		t.Errorf("Got invalid with on string: %#v", q.withs)
	}
}

func TestSetComment(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetComment(q, "my comment")

	if q.comment != "my comment" {
		t.Errorf("Got invalid comment: %s", q.comment)
	}
}

func TestSetAppendComment(t *testing.T) {
	t.Parallel()

	q := &Query{}
	SetAppendComment(q, "my comment")

	if q.appendComment != "my comment" {
		t.Errorf("Got invalid comment: %s", q.appendComment)
	}
}

func TestRemoveSoftDeleteWhere(t *testing.T) {
	t.Parallel()

	q := &Query{}
	AppendWhere(q, "a")
	AppendWhere(q, "b")
	AppendWhere(q, "deleted_at = false")
	AppendWhere(q, `"hello"."deleted_at" is null`)
	RemoveSoftDeleteWhere(q)

	q.removeSoftDeleteWhere()

	if len(q.where) != 3 {
		t.Error("should have removed one entry:", len(q.where))
	}

	if q.where[0].clause != "a" {
		t.Error("a was moved")
	}
	if q.where[1].clause != "b" {
		t.Error("b was moved")
	}
	if q.where[2].clause != "deleted_at = false" {
		t.Error("trick deleted_at was not found")
	}
	if t.Failed() {
		t.Logf("%#v\n", q.where)
	}

	q = &Query{}
	AppendWhere(q, "a")
	AppendWhere(q, "b")
	AppendWhere(q, `"hello"."deleted_at" is null`)
	AppendWhere(q, "deleted_at = false")
	RemoveSoftDeleteWhere(q)

	q.removeSoftDeleteWhere()

	if len(q.where) != 3 {
		t.Error("should have removed one entry:", len(q.where))
	}

	if q.where[0].clause != "a" {
		t.Error("a was moved")
	}
	if q.where[1].clause != "b" {
		t.Error("b was moved")
	}
	if q.where[2].clause != "deleted_at = false" {
		t.Error("trick deleted at did not replace the deleted_at is null entry")
	}
	if t.Failed() {
		t.Logf("%#v\n", q.where)
	}
}
