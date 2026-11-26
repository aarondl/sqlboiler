//go:build go1.27

// go-enums needs Go 1.27, so compiling the code generated for it can only be
// done when the tests run with at least that version.

package boilingcore

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/drivers/mocks"
	"github.com/aarondl/sqlboiler/v4/importers"
)

// goEnumsDriver is a mock driver with tables that use enums, which are
// translated the way the real drivers do with add-enum-types.
type goEnumsDriver struct {
	mocks.MockDriver
}

func init() {
	drivers.RegisterFromInit("mock-go-enums", &goEnumsDriver{})
}

func (goEnumsDriver) Assemble(drivers.Config) (*drivers.DBInfo, error) {
	id := drivers.Column{Name: "id", Type: "int", DBType: "integer"}
	pkey := &drivers.PrimaryKey{Name: "pkey", Columns: []string{"id"}}

	return &drivers.DBInfo{
		Dialect: drivers.Dialect{LQ: '"', RQ: '"', UseIndexPlaceholders: true},
		Tables: []drivers.Table{
			{
				Name: "events",
				PKey: pkey,
				Columns: []drivers.Column{
					id,
					{Name: "day", Type: "Workday", DBType: "enum.workday('monday','tuesday')"},
					{Name: "other_day", Type: "NullWorkday", DBType: "enum.workday('monday','tuesday')", Nullable: true},
					{Name: "mood", Type: "Mood", DBType: "enum.mood('happy','sad')", Default: "'happy'::mood"},
				},
			},
			{
				// Shares the Workday enum with the table above.
				Name: "meetings",
				PKey: pkey,
				Columns: []drivers.Column{
					id,
					{Name: "day", Type: "NullWorkday", DBType: "enum.workday('monday','tuesday')", Nullable: true},
				},
			},
		},
	}, nil
}

// goEnumsBehaviorTest runs inside the generated package.
const goEnumsBehaviorTest = `package models

import (
	"encoding/json"
	"testing"
)

func TestGoEnumsBehavior(t *testing.T) {
	var n NullWorkday
	if err := n.Scan(nil); err != nil || n.Valid {
		t.Fatalf("scan nil: valid=%v err=%v", n.Valid, err)
	}
	if err := n.Scan("monday"); err != nil || !n.Valid || n.Val != WorkdayMonday {
		t.Fatalf("scan monday: %+v err=%v", n, err)
	}
	if v, err := n.Value(); err != nil || v != "monday" {
		t.Fatalf("value: %v err=%v", v, err)
	}

	n = NullWorkday{}
	if err := n.Scan("sunday"); err == nil || n.Valid {
		t.Fatalf("scan of an unknown value: valid=%v err=%v", n.Valid, err)
	}

	if _, err := (Workday{}).Value(); err == nil {
		t.Fatal("expected an error saving the zero value")
	}
	if v, err := (NullWorkday{}).Value(); err != nil || v != nil {
		t.Fatalf("null value: %v err=%v", v, err)
	}

	b, err := json.Marshal(NullEnumFrom(WorkdayTuesday))
	if err != nil || string(b) != "\"tuesday\"" {
		t.Fatalf("marshal: %s err=%v", b, err)
	}
	if b, err = json.Marshal(NullWorkday{}); err != nil || string(b) != "null" {
		t.Fatalf("marshal null: %s err=%v", b, err)
	}
	if err = json.Unmarshal([]byte("\"sunday\""), &n); err == nil {
		t.Fatal("expected an error unmarshaling an unknown value")
	}
	if err = json.Unmarshal([]byte("null"), &n); err != nil || n.Valid {
		t.Fatalf("unmarshal null: valid=%v err=%v", n.Valid, err)
	}

	var w Workday
	w.Randomize(func() int64 { return 3 }, "", false)
	if !w.IsValid() {
		t.Fatal("randomize produced an invalid value")
	}
	if got := NullEnumFromPtr[Workday](nil); got.Valid {
		t.Fatal("expected a null value from a nil pointer")
	}
}
`

// TestGoEnumsCompile generates code for tables with enums with the go-enums
// style, compiles it against the real library and exercises the enum types.
func TestGoEnumsCompile(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	out, err := os.MkdirTemp("", "boil_go_enums")
	if err != nil {
		t.Fatalf("unable to create tempdir: %s", err)
	}
	defer func() {
		if t.Failed() {
			t.Log("go-enums test output:", out)
			return
		}
		os.RemoveAll(out)
	}()

	st, err := New(&Config{
		DriverName:     "mock-go-enums",
		PkgName:        "models",
		OutFolder:      out,
		NoTests:        true,
		AddEnumTypes:   true,
		EnumStyle:      EnumStyleGoEnums,
		EnumNullPrefix: "Null",
		DriverConfig:   map[string]any{drivers.ConfigSchema: "schema"},
		Imports:        importers.NewDefaultImports(),
	})
	if err != nil {
		t.Fatalf("unable to create State using config: %s", err)
	}
	if err := st.Run(); err != nil {
		t.Fatalf("unable to execute State.Run: %s", err)
	}

	if err := os.WriteFile(filepath.Join(out, "enums_behavior_test.go"), []byte(goEnumsBehaviorTest), 0o600); err != nil {
		t.Fatal(err)
	}

	goMod, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD cmd execution failed: %s", err)
	}

	for _, args := range [][]string{
		{"mod", "init", "github.com/aarondl/sqlboiler-test"},
		{"mod", "edit", fmt.Sprintf("-replace=github.com/aarondl/sqlboiler/v4=%s", filepath.Dir(string(goMod)))},
		{"mod", "tidy"},
		{"vet", "./..."},
		{"test", "./..."},
	} {
		buf := &bytes.Buffer{}
		cmd := exec.Command("go", args...)
		cmd.Dir = out
		cmd.Stdout = buf
		cmd.Stderr = buf

		if err := cmd.Run(); err != nil {
			t.Fatalf("go %v failed: %s\n%s", args, err, buf)
		}
	}
}
