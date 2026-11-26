package drivers

import (
	"fmt"
)

// Table metadata from the database schema.
type Table struct {
	Name string `json:"name"`
	// For dbs with real schemas, like Postgres.
	// Example value: "schema_name"."table_name"
	SchemaName string   `json:"schema_name"`
	Columns    []Column `json:"columns"`

	PKey  *PrimaryKey  `json:"p_key"`
	FKeys []ForeignKey `json:"f_keys"`

	IsJoinTable bool `json:"is_join_table"`

	ToOneRelationships  []ToOneRelationship  `json:"to_one_relationships"`
	ToManyRelationships []ToManyRelationship `json:"to_many_relationships"`

	// For views
	IsView           bool             `json:"is_view"`
	ViewCapabilities ViewCapabilities `json:"view_capabilities"`
}

type ViewCapabilities struct {
	CanInsert bool `json:"can_insert"`
	CanUpsert bool `json:"can_upsert"`
}

// GetTable by name. Panics if not found (for use in templates mostly).
func GetTable(tables []Table, name string) (tbl Table) {
	for _, t := range tables {
		if t.Name == name {
			return t
		}
	}

	panic(fmt.Sprintf("could not find table name: %s", name))
}

// GetColumn by name. Panics if not found (for use in templates mostly).
func (t Table) GetColumn(name string) (col Column) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}

	panic(fmt.Sprintf("could not find column name: %s", name))
}

// CanLastInsertID checks the following:
// 1. Is there only one primary key?
// 2. Does the primary key column have a default value?
// 3. Is the primary key column type one of uintX/intX?
// If the above is all true, this table can use LastInsertId
func (t Table) CanLastInsertID() bool {
	if t.PKey == nil || len(t.PKey.Columns) != 1 {
		return false
	}

	col := t.GetColumn(t.PKey.Columns[0])
	if len(col.Default) == 0 {
		return false
	}

	switch col.Type {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
	default:
		return false
	}

	return true
}

func (t Table) CanSoftDelete(deleteColumn string) bool {
	if deleteColumn == "" {
		deleteColumn = "deleted_at"
	}

	for _, column := range t.Columns {
		if column.Name == deleteColumn && column.Type == "null.Time" {
			return true
		}
	}
	return false
}

// TablesHaveEnums returns true if any column of the tables is an enum.
func TablesHaveEnums(tables []Table) bool {
	for _, table := range tables {
		for _, col := range table.Columns {
			if IsEnumDBType(col.DBType) {
				return true
			}
		}
	}
	return false
}

func TablesHaveNullableEnums(tables []Table) bool {
	for _, table := range tables {
		for _, col := range table.Columns {
			if col.Nullable && IsEnumDBType(col.DBType) {
				return true
			}
		}
	}
	return false
}
