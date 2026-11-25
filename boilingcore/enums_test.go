package boilingcore

import (
	"bytes"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/importers"
	"github.com/aarondl/sqlboiler/v4/templates"
)

func renderBoilTypes(t *testing.T, data *templateData) string {
	t.Helper()

	const name = "main/singleton/boil_types.go.tpl"
	tpls, err := loadTemplates([]lazyTemplate{
		{Name: name, Loader: assetLoader{fs: templates.Builtin, name: name}},
	}, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	if err := executeTemplate(buf, tpls.Template, name, data); err != nil {
		t.Fatal(err)
	}

	out, err := formatBuffer(bytes.NewBufferString("package models\n" + buf.String()))
	if err != nil {
		t.Fatalf("generated code does not format: %v\n%s", err, buf.String())
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "boil_types.go", out, parser.AllErrors); err != nil {
		t.Fatalf("generated code does not parse: %v\n%s", err, out)
	}

	return string(out)
}

func enumTables() []drivers.Table {
	return []drivers.Table{{
		Name: "events",
		Columns: []drivers.Column{
			{Name: "id", Type: "int", DBType: "integer"},
			{Name: "day", Type: "Workday", DBType: "enum.workday('monday','tuesday')"},
			{Name: "thing", Type: "NullWorkday", DBType: "enum.workday('monday','tuesday')", Nullable: true},
		},
	}}
}

func TestBoilTypesGoEnums(t *testing.T) {
	t.Parallel()

	out := renderBoilTypes(t, &templateData{
		PkgName:        "models",
		Tables:         enumTables(),
		AddEnumTypes:   true,
		EnumStyle:      EnumStyleGoEnums,
		EnumNullPrefix: "Null",
	})

	for _, want := range []string{
		"type Workday struct {\n\tenum.StringEnum[Workday]\n}",
		`enum.New[Workday]("monday")`,
		`enum.New[Workday]("tuesday")`,
		"type NullWorkday struct {",
		"return e.Val.Value()",
		"e.Val.Scan(value)",
		"e.Val.MarshalText()",
		"e.Val.UnmarshalText(text)",
		"func (e *Workday) Randomize(",
		"func (e *NullWorkday) Randomize(",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected generated code to contain %q:\n%s", want, out)
		}
	}

	// The members, IsValid, String and friends come from the library.
	for _, unwanted := range []string{
		"type Workday string",
		"func AllWorkday(",
		"func (e Workday) IsValid()",
		"func (e Workday) String()",
		"func (e Workday) Ordinal()",
		"convert.ConvertAssign",
	} {
		if strings.Contains(out, unwanted) {
			t.Errorf("expected generated code not to contain %q:\n%s", unwanted, out)
		}
	}
}

func TestBoilTypesConstStyle(t *testing.T) {
	t.Parallel()

	out := renderBoilTypes(t, &templateData{
		PkgName:        "models",
		Tables:         enumTables(),
		AddEnumTypes:   true,
		EnumNullPrefix: "Null",
	})

	for _, want := range []string{
		"type Workday string",
		"func AllWorkday()",
		"func (e Workday) IsValid() error",
		"convert.ConvertAssign",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected generated code to contain %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"go-enums", "enum.StringEnum", "Randomize"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("expected generated code not to contain %q:\n%s", unwanted, out)
		}
	}
}

func TestEnumStyleValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{name: "default"},
		{name: "const", config: Config{EnumStyle: EnumStyleConst}},
		{name: "const with enum types", config: Config{EnumStyle: EnumStyleConst, AddEnumTypes: true}},
		{name: "go-enums with enum types", config: Config{EnumStyle: EnumStyleGoEnums, AddEnumTypes: true}},
		{name: "go-enums without enum types", config: Config{EnumStyle: EnumStyleGoEnums}, wantErr: "add-enum-types"},
		{name: "unknown", config: Config{EnumStyle: "iota", AddEnumTypes: true}, wantErr: "unknown enum-style"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.validateEnumStyle()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Errorf("expected an error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func TestMergeEnumImportsGoEnums(t *testing.T) {
	t.Parallel()

	const libImport = `"github.com/paulo-raca/go-enums/enum"`
	const convertImport = `"github.com/aarondl/null/v8/convert"`

	tests := []struct {
		name         string
		enumStyle    string
		tables       []drivers.Table
		wantLib      bool
		wantConvert  bool
		wantNullable bool
	}{
		{name: "const, nullable", tables: enumTables(), wantConvert: true, wantNullable: true},
		{name: "go-enums, nullable", enumStyle: EnumStyleGoEnums, tables: enumTables(), wantLib: true, wantNullable: true},
		{
			name:      "go-enums, not nullable",
			enumStyle: EnumStyleGoEnums,
			tables: []drivers.Table{{Columns: []drivers.Column{
				{Name: "day", Type: "Workday", DBType: "enum.workday('monday')"},
			}}},
			wantLib: true,
		},
		{
			name:      "go-enums, no enums",
			enumStyle: EnumStyleGoEnums,
			tables:    []drivers.Table{{Columns: []drivers.Column{{Name: "id", Type: "int", DBType: "integer"}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &State{
				Config: &Config{AddEnumTypes: true, EnumStyle: tt.enumStyle, Imports: importers.Collection{}},
				Tables: tt.tables,
			}
			s.mergeEnumImports()

			got := s.Config.Imports.Singleton["boil_types"]
			has := func(l importers.List, v string) bool {
				for _, i := range l {
					if i == v {
						return true
					}
				}
				return false
			}

			if has(got.ThirdParty, libImport) != tt.wantLib {
				t.Errorf("go-enums import: want %v, got imports %v", tt.wantLib, got)
			}
			if has(got.ThirdParty, convertImport) != tt.wantConvert {
				t.Errorf("convert import: want %v, got imports %v", tt.wantConvert, got)
			}
			if has(got.Standard, `"database/sql/driver"`) != tt.wantNullable {
				t.Errorf("nullable imports: want %v, got imports %v", tt.wantNullable, got)
			}
		})
	}
}
