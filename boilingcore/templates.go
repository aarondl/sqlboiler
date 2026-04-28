package boilingcore

import (
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/aarondl/strmangle"
	"github.com/friendsofgo/errors"

	"github.com/aarondl/sqlboiler/v4/drivers"
)

// templateData for sqlboiler templates
type templateData struct {
	Tables  []drivers.Table
	Table   drivers.Table
	Aliases Aliases

	// Controls what names are output
	PkgName string
	Schema  string

	// Helps tune the output
	DriverName string
	Dialect    drivers.Dialect

	// LQ and RQ contain a quoted quote that allows us to write
	// the templates more easily.
	LQ string
	RQ string

	// Control various generation features
	// NoOutputSchema, which is also used for generation, is defined in the drivers configuration
	AddGlobal             bool
	AddPanic              bool
	AddSoftDeletes        bool
	AddEnumTypes          bool
	SkipReplacedEnumTypes bool
	EnumNullPrefix        string
	NoContext             bool
	NoHooks               bool
	NoAutoTimestamps      bool
	NoRowsAffected        bool
	NoDriverTemplates     bool
	NoBackReferencing     bool
	NoRelationGetters     bool
	AlwaysWrapErrors      bool

	// Tags control which tags are added to the struct
	Tags []string

	// RelationTag controls the value of the tags for the Relationship struct
	RelationTag string

	// Generate struct tags as camelCase or snake_case
	// Deprecated: use StructTagCases instead.
	StructTagCasing string

	// Generate struct tags as camelCase or snake_case
	StructTagCases StructTagCases

	// Contains field names that should have tags values set to '-'
	TagIgnore map[string]struct{}

	// OutputDirDepth is used to find sqlboiler config file
	OutputDirDepth int

	// Hacky state for where clauses to avoid having to do type-based imports
	// for singletons
	DBTypes once

	// StringFuncs are usable in templates with stringMap
	StringFuncs map[string]func(string) string

	// AutoColumns set the name of the columns for auto timestamps and soft deletes
	AutoColumns AutoColumns

	// Enum types which will not be generated, because they were replaced.
	DiscardedEnumTypes []string
}

func (t templateData) Quotes(s string) string {
	return fmt.Sprintf("%s%s%s", t.LQ, s, t.RQ)
}

func (t templateData) QuoteMap(s []string) []string {
	return strmangle.StringMap(t.Quotes, s)
}

func (t templateData) SchemaTable(table string) string {
	return strmangle.SchemaTable(t.LQ, t.RQ, t.Dialect.UseSchema, t.Schema, table)
}

type templateList struct {
	*template.Template
}

type templateNameList []string

func (t templateNameList) Len() int {
	return len(t)
}

func (t templateNameList) Swap(k, j int) {
	t[k], t[j] = t[j], t[k]
}

func (t templateNameList) Less(k, j int) bool {
	// Make sure "struct" goes to the front
	if t[k] == "struct.tpl" {
		return true
	}

	res := strings.Compare(t[k], t[j])
	return res <= 0
}

// Templates returns the name of all the templates defined in the template list
func (t templateList) Templates() []string {
	tplList := t.Template.Templates()

	if len(tplList) == 0 {
		return nil
	}

	ret := make([]string, 0, len(tplList))
	for _, tpl := range tplList {
		if name := tpl.Name(); strings.HasSuffix(name, ".tpl") {
			ret = append(ret, name)
		}
	}

	sort.Sort(templateNameList(ret))

	return ret
}

func loadTemplates(
	lazyTemplates []lazyTemplate, testTemplates bool, customFuncs template.FuncMap,
) (*templateList, error) {
	tpl := template.New("")

	for _, t := range lazyTemplates {
		firstDir := strings.Split(t.Name, string(filepath.Separator))[0]
		isTest := firstDir == "test" || strings.HasSuffix(firstDir, "_test")
		if testTemplates && !isTest || !testTemplates && isTest {
			continue
		}

		byt, err := t.Loader.Load()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load template: %s", t.Name)
		}

		_, err = tpl.New(t.Name).
			Funcs(sprig.GenericFuncMap()).
			Funcs(templateFunctions).
			Funcs(customFuncs).
			Parse(string(byt))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse template: %s", t.Name)
		}
	}

	return &templateList{Template: tpl}, nil
}

type lazyTemplate struct {
	Name   string         `json:"name"`
	Loader templateLoader `json:"loader"`
}

type templateLoader interface {
	encoding.TextMarshaler
	Load() ([]byte, error)
}

type fileLoader string

func (f fileLoader) Load() ([]byte, error) {
	fname := string(f)
	b, err := os.ReadFile(fname)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load template: %s", fname)
	}
	return b, nil
}

func (f fileLoader) MarshalText() ([]byte, error) {
	return []byte(f.String()), nil
}

func (f fileLoader) String() string {
	return "file:" + string(f)
}

type base64Loader string

func (b base64Loader) Load() ([]byte, error) {
	byt, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode driver's template, should be base64)")
	}
	return byt, nil
}

func (b base64Loader) MarshalText() ([]byte, error) {
	return []byte(b.String()), nil
}

func (b base64Loader) String() string {
	byt, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		panic("trying to debug output base64 loader, but was not proper base64!")
	}
	sha := sha256.Sum256(byt)
	return fmt.Sprintf("base64:(sha256 of content): %x", sha)
}

type assetLoader struct {
	fs   fs.FS
	name string
}

func (a assetLoader) Load() ([]byte, error) {
	return fs.ReadFile(a.fs, string(a.name))
}

func (a assetLoader) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

func (a assetLoader) String() string {
	return "asset:" + string(a.name)
}

// set is to stop duplication from named enums, allowing a template loop
// to keep some state
type once map[string]struct{}

func newOnce() once {
	return make(once)
}

func (o once) Has(s string) bool {
	_, ok := o[s]
	return ok
}

func (o once) Put(s string) bool {
	if _, ok := o[s]; ok {
		return false
	}

	o[s] = struct{}{}
	return true
}

// templateStringMappers are placed into the data to make it easy to use the
// stringMap function.
var templateStringMappers = map[string]func(string) string{
	// String ops
	"quoteWrap":       func(a string) string { return fmt.Sprintf(`%q`, a) },
	"safeQuoteWrap":   func(a string) string { return fmt.Sprintf(`\"%s\"`, a) },
	"replaceReserved": strmangle.ReplaceReservedWords,

	// Casing
	"titleCase": strmangle.TitleCase,
	"camelCase": strmangle.CamelCase,
}

var goVarnameReplacer = strings.NewReplacer("[", "_", "]", "_", ".", "_")

// templateFunctions is a map of some helper functions that get passed into the
// templates. If you wish to pass a new function into your own template,
// you can add that with Config.CustomTemplateFuncs
var templateFunctions = template.FuncMap{
	// String ops
	"quoteWrap": func(s string) string { return fmt.Sprintf(`"%s"`, s) },
	"id":        strmangle.Identifier,
	"goVarname": goVarnameReplacer.Replace,

	// Pluralization
	"singular": strmangle.Singular,
	"plural":   strmangle.Plural,

	// Casing
	"titleCase": strmangle.TitleCase,
	"camelCase": strmangle.CamelCase,
	"ignore":    strmangle.Ignore,

	// String Slice ops
	"join":                func(sep string, slice []string) string { return strings.Join(slice, sep) },
	"joinSlices":          strmangle.JoinSlices,
	"stringMap":           strmangle.StringMap,
	"prefixStringSlice":   strmangle.PrefixStringSlice,
	"containsAny":         strmangle.ContainsAny,
	"generateTags":        strmangle.GenerateTags,
	"generateTagWithCase": generateTagWithCase,
	"generateIgnoreTags":  strmangle.GenerateIgnoreTags,

	// Enum ops
	"parseEnumName": strmangle.ParseEnumName,
	"parseEnumVals": strmangle.ParseEnumVals,
	"onceNew":       newOnce,
	"oncePut":       once.Put,
	"onceHas":       once.Has,
	"isEnumDBType":  drivers.IsEnumDBType,

	// String Map ops
	"makeStringMap": strmangle.MakeStringMap,

	// Set operations
	"setInclude": strmangle.SetInclude,

	// Database related mangling
	"whereClause": strmangle.WhereClause,

	// Alias and text helping
	"aliasCols":              func(ta TableAlias) func(string) string { return ta.Column },
	"usesPrimitives":         usesPrimitives,
	"isPrimitive":            isPrimitive,
	"isNullPrimitive":        isNullPrimitive,
	"convertNullToPrimitive": convertNullToPrimitive,
	"splitLines": func(a string) []string {
		if a == "" {
			return nil
		}
		return strings.Split(strings.TrimSpace(a), "\n")
	},

	// dbdrivers ops
	"filterColumnsByAuto":    drivers.FilterColumnsByAuto,
	"filterColumnsByDefault": drivers.FilterColumnsByDefault,
	"filterColumnsByEnum":    drivers.FilterColumnsByEnum,
	"sqlColDefinitions":      drivers.SQLColDefinitions,
	"columnNames":            drivers.ColumnNames,
	"columnDBTypes":          drivers.ColumnDBTypes,
	"getTable":               drivers.GetTable,
}

// camelCaseNoInitialisms converts a snake_case string to camelCase for struct
// tags. Unlike strmangle.CamelCase, it does not uppercase common Go initialisms
// such as "ID" or "URL". For example, "account_id" becomes "accountId" instead
// of "accountID", following the conventional camelCase used in JSON APIs.
func camelCaseNoInitialisms(name string) string {
	if name == "" {
		return ""
	}

	buf := strmangle.GetBuffer()
	defer strmangle.PutBuffer(buf)

	firstWritten := false
	for _, word := range strings.Split(name, "_") {
		if word == "" {
			continue
		}
		if !firstWritten {
			buf.WriteString(strings.ToLower(word))
			firstWritten = true
		} else {
			buf.WriteString(strings.ToUpper(word[:1]))
			if len(word) > 1 {
				buf.WriteString(strings.ToLower(word[1:]))
			}
		}
	}

	return buf.String()
}

func generateTagWithCase(tagName, tagValue, alias string, c TagCase, nullable bool) string {
	buf := strmangle.GetBuffer()
	defer strmangle.PutBuffer(buf)

	buf.WriteString(tagName)
	buf.WriteString(`:"`)
	switch c {
	case TagCaseSnake:
		// we use snake case by default, so we can simply render the value to the buffer
		buf.WriteString(tagValue)
	case TagCaseTitle:
		buf.WriteString(strmangle.TitleCase(tagValue))
	case TagCaseCamel:
		buf.WriteString(camelCaseNoInitialisms(tagValue))
	case TagCaseAlias:
		buf.WriteString(alias)
	default:
		buf.WriteString(tagValue)
	}

	if nullable {
		buf.WriteString(",omitempty")
	}

	buf.WriteString(`" `)

	return buf.String()
}
