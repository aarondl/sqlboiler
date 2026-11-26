package boilingcore

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/friendsofgo/errors"
	"github.com/spf13/cast"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/importers"
)

type TagCase string

const (
	TagCaseCamel TagCase = "camel"
	TagCaseSnake TagCase = "snake"
	TagCaseTitle TagCase = "title"
	TagCaseAlias TagCase = "alias"
)

// Values of Config.EnumStyle.
const (
	// EnumStyleConst generates enums as a string type and constants. This is
	// the default, also when EnumStyle is empty.
	EnumStyleConst = "const"
	// EnumStyleGoEnums generates enums with github.com/paulo-raca/go-enums.
	// It requires AddEnumTypes.
	EnumStyleGoEnums = "go-enums"
)

// Config for the running of the commands
type Config struct {
	DriverName   string         `toml:"driver_name,omitempty" json:"driver_name,omitempty"`
	DriverConfig drivers.Config `toml:"driver_config,omitempty" json:"driver_config,omitempty"`

	PkgName               string   `toml:"pkg_name,omitempty" json:"pkg_name,omitempty"`
	OutFolder             string   `toml:"out_folder,omitempty" json:"out_folder,omitempty"`
	TemplateDirs          []string `toml:"template_dirs,omitempty" json:"template_dirs,omitempty"`
	Tags                  []string `toml:"tags,omitempty" json:"tags,omitempty"`
	Replacements          []string `toml:"replacements,omitempty" json:"replacements,omitempty"`
	Debug                 bool     `toml:"debug,omitempty" json:"debug,omitempty"`
	AddGlobal             bool     `toml:"add_global,omitempty" json:"add_global,omitempty"`
	AddPanic              bool     `toml:"add_panic,omitempty" json:"add_panic,omitempty"`
	AddSoftDeletes        bool     `toml:"add_soft_deletes,omitempty" json:"add_soft_deletes,omitempty"`
	AddEnumTypes          bool     `toml:"add_enum_types,omitempty" json:"add_enum_types,omitempty"`
	SkipReplacedEnumTypes bool     `toml:"skip_replaced_enum_types,omitempty" json:"skip_replaced_enum_types,omitempty"`
	EnumNullPrefix        string   `toml:"enum_null_prefix,omitempty" json:"enum_null_prefix,omitempty"`
	EnumStyle             string   `toml:"enum_style,omitempty" json:"enum_style,omitempty"`
	NoContext             bool     `toml:"no_context,omitempty" json:"no_context,omitempty"`
	NoTests               bool     `toml:"no_tests,omitempty" json:"no_tests,omitempty"`
	NoHooks               bool     `toml:"no_hooks,omitempty" json:"no_hooks,omitempty"`
	NoAutoTimestamps      bool     `toml:"no_auto_timestamps,omitempty" json:"no_auto_timestamps,omitempty"`
	NoRowsAffected        bool     `toml:"no_rows_affected,omitempty" json:"no_rows_affected,omitempty"`
	NoDriverTemplates     bool     `toml:"no_driver_templates,omitempty" json:"no_driver_templates,omitempty"`
	NoBackReferencing     bool     `toml:"no_back_reference,omitempty" json:"no_back_reference,omitempty"`
	NoRelationGetters     bool     `toml:"no_relation_getters,omitempty" json:"no_relation_getters,omitempty"`
	AlwaysWrapErrors      bool     `toml:"always_wrap_errors,omitempty" json:"always_wrap_errors,omitempty"`
	Wipe                  bool     `toml:"wipe,omitempty" json:"wipe,omitempty"`

	StructTagCases StructTagCases `toml:"struct_tag_cases,omitempty" json:"struct_tag_cases,omitempty"`

	// StructTagCasing is a legacy config field, which will be migrated to StructTagCases in the future.
	// When struct-tag-casing is defined, it will be converted to StructTagCases
	// Deprecated: use StructTagCases instead.
	StructTagCasing string `toml:"struct_tag_casing,omitempty" json:"struct_tag_casing,omitempty"`

	RelationTag string   `toml:"relation_tag,omitempty" json:"relation_tag,omitempty"`
	TagIgnore   []string `toml:"tag_ignore,omitempty" json:"tag_ignore,omitempty"`

	Imports importers.Collection `toml:"imports,omitempty" json:"imports,omitempty"`

	DiscardedEnumTypes []string

	DefaultTemplates    fs.FS            `toml:"-" json:"-"`
	CustomTemplateFuncs template.FuncMap `toml:"-" json:"-"`

	Aliases      Aliases              `toml:"aliases,omitempty" json:"aliases,omitempty"`
	TypeReplaces []TypeReplace        `toml:"type_replaces,omitempty" json:"type_replaces,omitempty"`
	AutoColumns  AutoColumns          `toml:"auto_columns,omitempty" json:"auto_columns,omitempty"`
	Inflections  Inflections          `toml:"inflections,omitempty" json:"inflections,omitempty"`
	ForeignKeys  []drivers.ForeignKey `toml:"foreign_keys,omitempty" json:"foreign_keys,omitempty" `

	StrictVerifyModVersion bool `toml:"strict_verify_mod_version,omitempty" json:"strict_verify_mod_version"`

	Version string `toml:"version" json:"version"`
}

type AutoColumns struct {
	Created string `toml:"created,omitempty" json:"created,omitempty"`
	Updated string `toml:"updated,omitempty" json:"updated,omitempty"`
	Deleted string `toml:"deleted,omitempty" json:"deleted,omitempty"`
}

type StructTagCases struct {
	Json TagCase `toml:"json,omitempty" json:"json,omitempty"`
	Yaml TagCase `toml:"yaml,omitempty" json:"yaml,omitempty"`
	Toml TagCase `toml:"toml,omitempty" json:"toml,omitempty"`
	Boil TagCase `toml:"boil,omitempty" json:"boil,omitempty"`
}

// TypeReplace replaces a column type with something else
type TypeReplace struct {
	Tables  []string       `toml:"tables,omitempty" json:"tables,omitempty"`
	Views   []string       `toml:"views,omitempty" json:"views,omitempty"`
	Match   drivers.Column `toml:"match,omitempty" json:"match,omitempty"`
	Replace drivers.Column `toml:"replace,omitempty" json:"replace,omitempty"`
	Imports importers.Set  `toml:"imports,omitempty" json:"imports,omitempty"`
}

type Inflections struct {
	Plural        map[string]string
	PluralExact   map[string]string
	Singular      map[string]string
	SingularExact map[string]string
	Irregular     map[string]string
}

// validateEnumStyle checks that EnumStyle is known and consistent with the
// other enum settings.
func (c *Config) validateEnumStyle() error {
	switch c.EnumStyle {
	case "", EnumStyleConst:
		return nil
	case EnumStyleGoEnums:
		if !c.AddEnumTypes {
			return errors.Errorf("enum-style %q requires add-enum-types to be enabled", c.EnumStyle)
		}
		return nil
	default:
		return errors.Errorf("unknown enum-style %q, must be %q or %q", c.EnumStyle, EnumStyleConst, EnumStyleGoEnums)
	}
}

// OutputDirDepth returns depth of output directory
func (c *Config) OutputDirDepth() int {
	d := filepath.ToSlash(filepath.Clean(c.OutFolder))
	if d == "." {
		return 0
	}

	return strings.Count(d, "/") + 1
}

// ConvertAliases is necessary because viper
//
// It also supports two different syntaxes, because of viper:
//
//	[aliases.tables.table_name]
//	fields... = "values"
//	  [aliases.tables.columns]
//	  colname = "alias"
//	  [aliases.tables.relationships.fkey_name]
//	  local   = "x"
//	  foreign = "y"
//
// Or alternatively (when toml key names or viper's
// lowercasing of key names gets in the way):
//
//	[[aliases.tables]]
//	name = "table_name"
//	fields... = "values"
//	  [[aliases.tables.columns]]
//	  name  = "colname"
//	  alias = "alias"
//	  [[aliases.tables.relationships]]
//	  name    = "fkey_name"
//	  local   = "x"
//	  foreign = "y"
func ConvertAliases(i any) (a Aliases) {
	if i == nil {
		return a
	}

	topLevel := cast.ToStringMap(i)

	tablesIntf := topLevel["tables"]

	iterateMapOrSlice(tablesIntf, func(name string, tIntf any) {
		if a.Tables == nil {
			a.Tables = make(map[string]TableAlias)
		}

		t := cast.ToStringMap(tIntf)

		var ta TableAlias

		if s := t["up_plural"]; s != nil {
			ta.UpPlural = s.(string)
		}
		if s := t["up_singular"]; s != nil {
			ta.UpSingular = s.(string)
		}
		if s := t["down_plural"]; s != nil {
			ta.DownPlural = s.(string)
		}
		if s := t["down_singular"]; s != nil {
			ta.DownSingular = s.(string)
		}

		if colsIntf, ok := t["columns"]; ok {
			ta.Columns = make(map[string]string)

			iterateMapOrSlice(colsIntf, func(name string, colIntf any) {
				var alias string
				switch col := colIntf.(type) {
				case map[string]any, map[any]any:
					cmap := cast.ToStringMap(colIntf)
					alias = cmap["alias"].(string)
				case string:
					alias = col
				}
				ta.Columns[name] = alias
			})
		}

		relationshipsIntf, ok := t["relationships"]
		if ok {
			iterateMapOrSlice(relationshipsIntf, func(name string, rIntf any) {
				if ta.Relationships == nil {
					ta.Relationships = make(map[string]RelationshipAlias)
				}

				var ra RelationshipAlias
				rel := cast.ToStringMap(rIntf)

				if s := rel["local"]; s != nil {
					ra.Local = s.(string)
				}
				if s := rel["foreign"]; s != nil {
					ra.Foreign = s.(string)
				}

				ta.Relationships[name] = ra
			})
		}

		a.Tables[name] = ta
	})

	return a
}

func iterateMapOrSlice(mapOrSlice any, fn func(name string, obj any)) {
	switch t := mapOrSlice.(type) {
	case map[string]any, map[any]any:
		tmap := cast.ToStringMap(mapOrSlice)
		for name, table := range tmap {
			fn(name, table)
		}
	case []any:
		for _, intf := range t {
			obj := cast.ToStringMap(intf)
			name := obj["name"].(string)
			fn(name, intf)
		}
	}
}

// ConvertTypeReplace is necessary because viper
func ConvertTypeReplace(i any) []TypeReplace {
	if i == nil {
		return nil
	}

	intfArray := i.([]any)
	var replaces []TypeReplace
	for _, r := range intfArray {
		replaceIntf := cast.ToStringMap(r)
		replace := TypeReplace{}

		if replaceIntf["match"] == nil || replaceIntf["replace"] == nil {
			panic("replace types must specify both match and replace")
		}

		replace.Match = columnFromInterface(replaceIntf["match"])
		replace.Replace = columnFromInterface(replaceIntf["replace"])

		replace.Tables = tablesOfTypeReplace(replaceIntf["match"])

		if imps := replaceIntf["imports"]; imps != nil {
			imps = cast.ToStringMap(imps)
			var err error
			replace.Imports, err = importers.SetFromInterface(imps)
			if err != nil {
				panic(err)
			}
		}

		replaces = append(replaces, replace)
	}

	return replaces
}

func tablesOfTypeReplace(i any) []string {
	tables := []string{}

	m := cast.ToStringMap(i)
	if s := m["tables"]; s != nil {
		tables = cast.ToStringSlice(s)
	}

	return tables
}

func columnFromInterface(i any) (col drivers.Column) {
	m := cast.ToStringMap(i)
	if s := m["name"]; s != nil {
		col.Name = s.(string)
	}
	if s := m["type"]; s != nil {
		col.Type = s.(string)
	}
	if s := m["db_type"]; s != nil {
		col.DBType = s.(string)
	}
	if s := m["udt_name"]; s != nil {
		col.UDTName = s.(string)
	}
	if s := m["full_db_type"]; s != nil {
		col.FullDBType = s.(string)
	}
	if s := m["arr_type"]; s != nil {
		col.ArrType = new(string)
		*col.ArrType = s.(string)
	}
	if s := m["domain_name"]; s != nil {
		col.DomainName = new(string)
		*col.DomainName = s.(string)
	}
	if b := m["auto_generated"]; b != nil {
		col.AutoGenerated = b.(bool)
	}
	if b := m["nullable"]; b != nil {
		col.Nullable = b.(bool)
	}

	return col
}

// ConvertForeignKeys is necessary because viper
//
// It also supports two different syntaxes, because of viper:
//
//	[foreign_keys.fk_1]
//	table = "table_name"
//	column = "column_name"
//	foreign_table = "foreign_table_name"
//	foreign_column = "foreign_column_name"
//
// Or alternatively (when toml key names or viper's
// lowercasing of key names gets in the way):
//
//	[[foreign_keys]]
//	name = "fk_1"
//	table = "table_name"
//	column = "column_name"
//	foreign_table = "foreign_table_name"
//	foreign_column = "foreign_column_name"
func ConvertForeignKeys(i any) (fks []drivers.ForeignKey) {
	if i == nil {
		return nil
	}

	iterateMapOrSlice(i, func(name string, obj any) {
		t := cast.ToStringMap(obj)

		fk := drivers.ForeignKey{
			Table:         cast.ToString(t["table"]),
			Name:          name,
			Column:        cast.ToString(t["column"]),
			ForeignTable:  cast.ToString(t["foreign_table"]),
			ForeignColumn: cast.ToString(t["foreign_column"]),
		}
		if err := validateForeignKey(fk); err != nil {
			panic(errors.Errorf("invalid foreign key %s: %s", name, err))
		}
		fks = append(fks, fk)
	})

	if err := validateDuplicateForeignKeys(fks); err != nil {
		panic(errors.Errorf("invalid foreign keys: %s", err))
	}

	return fks
}

func validateForeignKey(fk drivers.ForeignKey) error {
	if fk.Name == "" {
		return errors.New("foreign key must have a name")
	}
	if fk.Table == "" {
		return errors.New("foreign key must have a table")
	}
	if fk.Column == "" {
		return errors.New("foreign key must have a column")
	}
	if fk.ForeignTable == "" {
		return errors.New("foreign key must have a foreign table")
	}
	if fk.ForeignColumn == "" {
		return errors.New("foreign key must have a foreign column")
	}
	return nil
}

func validateDuplicateForeignKeys(fks []drivers.ForeignKey) error {
	fkMap := make(map[string]drivers.ForeignKey)
	for _, fk := range fks {
		key := fmt.Sprintf("%s.%s", fk.Table, fk.Column)
		if _, ok := fkMap[key]; ok {
			return errors.Errorf("duplicate foreign key name: %s", fk.Name)
		}
		fkMap[key] = fk
	}
	return nil
}