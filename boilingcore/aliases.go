package boilingcore

import (
	"fmt"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/strmangle"
)

var builtInTableTokens = map[string]struct{}{
	"TableNames":  {},
	"ViewNames":   {},
	"NewQuery":    {},
	"M":           {},
	"ErrSyncFail": {},
}

var initDefaultInflectionsOnce sync.Once

func initDefaultInflections() {
	initDefaultInflectionsOnce.Do(func() {
		ruleset := strmangle.GetBoilRuleset()
		if ruleset != nil {
			ruleset.AddSingular("personas", "persona")
		}
	})
}

func init() {
	initDefaultInflections()
}

// Aliases defines aliases for the generation run
type Aliases struct {
	Tables map[string]TableAlias `toml:"tables,omitempty" json:"tables,omitempty"`
}

// TableAlias defines the spellings for a table name in Go
type TableAlias struct {
	UpPlural     string `toml:"up_plural,omitempty" json:"up_plural,omitempty"`
	UpSingular   string `toml:"up_singular,omitempty" json:"up_singular,omitempty"`
	DownPlural   string `toml:"down_plural,omitempty" json:"down_plural,omitempty"`
	DownSingular string `toml:"down_singular,omitempty" json:"down_singular,omitempty"`

	Columns       map[string]string            `toml:"columns,omitempty" json:"columns,omitempty"`
	Relationships map[string]RelationshipAlias `toml:"relationships,omitempty" json:"relationships,omitempty"`
}

// RelationshipAlias defines the naming for both sides of
// a foreign key.
type RelationshipAlias struct {
	Local   string `toml:"local,omitempty" json:"local,omitempty"`
	Foreign string `toml:"foreign,omitempty" json:"foreign,omitempty"`
}

// FillAliases takes the table information from the driver
// and fills in aliases where the user has provided none.
//
// This leaves us with a complete list of Go names for all tables,
// columns, and relationships.
func FillAliases(a *Aliases, tables []drivers.Table) {
	initDefaultInflections()

	if a.Tables == nil {
		a.Tables = make(map[string]TableAlias)
	}

	for _, t := range tables {
		if t.IsJoinTable {
			jt, ok := a.Tables[t.Name]
			if !ok {
				a.Tables[t.Name] = TableAlias{Relationships: make(map[string]RelationshipAlias)}
			} else if jt.Relationships == nil {
				jt.Relationships = make(map[string]RelationshipAlias)
			}
			continue
		}

		table := a.Tables[t.Name]

		userSpecifiedUpPlural := len(table.UpPlural) != 0
		userSpecifiedUpSingular := len(table.UpSingular) != 0

		if !userSpecifiedUpPlural {
			table.UpPlural = strmangle.TitleCase(strmangle.Plural(t.Name))
		}
		if !userSpecifiedUpSingular {
			table.UpSingular = strmangle.TitleCase(strmangle.Singular(t.Name))
		}
		if len(table.DownPlural) == 0 {
			table.DownPlural = strmangle.CamelCase(strmangle.Plural(t.Name))
		}
		if len(table.DownSingular) == 0 {
			table.DownSingular = strmangle.CamelCase(strmangle.Singular(t.Name))
		}

		// Resolve collisions between generated table struct helpers and
		// internal query helper names when singular and plural forms coincide.
		if table.UpSingular == table.UpPlural {
			if !userSpecifiedUpSingular && strings.HasSuffix(t.Name, "s") && !strings.HasSuffix(t.Name, "ss") && len(t.Name) > 1 {
				trimmed := strings.TrimSuffix(t.Name, "s")
				table.UpSingular = strmangle.TitleCase(trimmed)
				table.DownSingular = strmangle.CamelCase(trimmed)
			} else if !userSpecifiedUpPlural {
				table.UpPlural += "Records"
				table.DownPlural = strmangle.CamelCase(table.UpPlural)
			} else if !userSpecifiedUpSingular {
				table.UpSingular += "Record"
				table.DownSingular = strmangle.CamelCase(table.UpSingular)
			} else {
				table.UpPlural += "Records"
				table.DownPlural = strmangle.CamelCase(table.UpPlural)
			}
		}

		// Resolve collisions with built-in generator tokens
		if _, ok := builtInTableTokens[table.UpPlural]; ok {
			table.UpPlural += "Query"
			table.DownPlural = strmangle.CamelCase(table.UpPlural)
		}
		if _, ok := builtInTableTokens[table.UpSingular]; ok {
			table.UpSingular += "Record"
			table.DownSingular = strmangle.CamelCase(table.UpSingular)
		}

		if table.Columns == nil {
			table.Columns = make(map[string]string)
		}
		if table.Relationships == nil {
			table.Relationships = make(map[string]RelationshipAlias)
		}

		for _, c := range t.Columns {
			if _, ok := table.Columns[c.Name]; !ok {
				table.Columns[c.Name] = strmangle.TitleCase(c.Name)
			}

			r, _ := utf8.DecodeRuneInString(table.Columns[c.Name])
			if unicode.IsNumber(r) {
				table.Columns[c.Name] = "C" + table.Columns[c.Name]
			}
		}

		a.Tables[t.Name] = table

		for _, k := range t.FKeys {
			r := table.Relationships[k.Name]
			if len(r.Local) != 0 && len(r.Foreign) != 0 {
				continue
			}

			local, foreign := txtNameToOne(k)
			if len(r.Local) == 0 {
				r.Local = local
			}
			if len(r.Foreign) == 0 {
				r.Foreign = foreign
			}

			table.Relationships[k.Name] = r
		}
	}

	// Disambiguate collisions across tables in the slice
	usedSingulars := make(map[string]string)
	usedPlurals := make(map[string]string)

	for _, t := range tables {
		if t.IsJoinTable {
			continue
		}
		table := a.Tables[t.Name]

		// Disambiguate if UpSingular collides with another table's <UpSingular>Slice type
		for otherName, otherTable := range a.Tables {
			if otherName == t.Name {
				continue
			}
			if table.UpSingular == otherTable.UpSingular+"Slice" {
				table.UpSingular += "Record"
				table.DownSingular = strmangle.CamelCase(table.UpSingular)
				break
			}
		}

		// Disambiguate duplicate UpSingular across different tables
		if origTable, exists := usedSingulars[table.UpSingular]; exists && origTable != t.Name {
			table.UpSingular += "Record"
			table.DownSingular = strmangle.CamelCase(table.UpSingular)
		}
		usedSingulars[table.UpSingular] = t.Name

		// Disambiguate duplicate UpPlural across different tables
		if origTable, exists := usedPlurals[table.UpPlural]; exists && origTable != t.Name {
			table.UpPlural += "Records"
			table.DownPlural = strmangle.CamelCase(table.UpPlural)
		}
		usedPlurals[table.UpPlural] = t.Name

		a.Tables[t.Name] = table
	}

	for _, t := range tables {
		if !t.IsJoinTable {
			continue
		}

		table := a.Tables[t.Name]

		lhs := t.FKeys[0]
		rhs := t.FKeys[1]

		lhsAlias, lhsOK := table.Relationships[lhs.Name]
		rhsAlias, rhsOK := table.Relationships[rhs.Name]

		if lhsOK && len(lhsAlias.Local) != 0 && len(lhsAlias.Foreign) != 0 &&
			rhsOK && len(rhsAlias.Local) != 0 && len(rhsAlias.Foreign) != 0 {
			continue
		}

		// Here we actually reverse the meaning of local/foreign to be
		// consistent with the way normal one-to-many relationships are done.
		// That's to say local = the side with the foreign key. Now in a many-to-many
		// if we were able to not have a join table our foreign key say "videos_id"
		// would be on the tags table. Hence the relationships should look like:
		// videos_tags.relationships.fk_video_id.local   = "Tags"
		// videos_tags.relationships.fk_video_id.foreign = "Videos"
		// Consistent, yes. Confusing? Also yes.

		lhsName, rhsName := txtNameToMany(lhs, rhs)

		if len(lhsAlias.Local) != 0 {
			rhsName = lhsAlias.Local
		} else if len(rhsAlias.Local) != 0 {
			lhsName = rhsAlias.Local
		}

		if len(lhsAlias.Foreign) != 0 {
			lhsName = lhsAlias.Foreign
		} else if len(rhsAlias.Foreign) != 0 {
			rhsName = rhsAlias.Foreign
		}

		if len(lhsAlias.Local) == 0 {
			lhsAlias.Local = rhsName
		}
		if len(lhsAlias.Foreign) == 0 {
			lhsAlias.Foreign = lhsName
		}
		if len(rhsAlias.Local) == 0 {
			rhsAlias.Local = lhsName
		}
		if len(rhsAlias.Foreign) == 0 {
			rhsAlias.Foreign = rhsName
		}

		table.Relationships[lhs.Name] = lhsAlias
		table.Relationships[rhs.Name] = rhsAlias
	}
}

// Table gets a table alias, panics if not found.
func (a Aliases) Table(table string) TableAlias {
	t, ok := a.Tables[table]
	if !ok {
		panic("could not find table aliases for: " + table)
	}

	return t
}

// Column get's a column's aliased name, panics if not found.
func (t TableAlias) Column(column string) string {
	c, ok := t.Columns[column]
	if !ok {
		panic(fmt.Sprintf("could not find column alias for: %s.%s", t.UpSingular, column))
	}

	return c
}

// Relationship looks up a relationship, panics if not found.
func (t TableAlias) Relationship(fkey string) RelationshipAlias {
	r, ok := t.Relationships[fkey]
	if !ok {
		panic(fmt.Sprintf("could not find relationship alias for: %s.%s", t.UpSingular, fkey))
	}

	return r
}

// ManyRelationship looks up a relationship alias, panics if not found.
// It will first try to look up a join table relationship, then it will
// try a normal one-to-many relationship. That's to say joinTable/joinTableFKey
// are used if they're not empty.
//
// This allows us to skip additional conditionals in the templates.
func (a Aliases) ManyRelationship(table, fkey, joinTable, joinTableFKey string) RelationshipAlias {
	var lookupTable, lookupFKey string
	if len(joinTable) != 0 {
		lookupTable, lookupFKey = joinTable, joinTableFKey
	} else {
		lookupTable, lookupFKey = table, fkey
	}

	t := a.Table(lookupTable)
	return t.Relationship(lookupFKey)
}
