package driver

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var (
	flagOverwriteGolden = flag.Bool("overwrite-golden", false, "Overwrite the golden file with the current execution results")
)

func createSQLiteDB(t *testing.T, path string, sql []byte) {
	t.Helper()
	out := &bytes.Buffer{}
	cmd := exec.Command("sqlite3", path)
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.Stdin = bytes.NewReader(sql)
	if err := cmd.Run(); err != nil {
		t.Logf("sqlite output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
}

func TestDriver(t *testing.T) {
	rand.New(rand.NewSource(time.Now().Unix()))
	b, err := os.ReadFile("testdatabase.sql")
	if err != nil {
		t.Fatal(err)
	}

	tmpName := filepath.Join(os.TempDir(), fmt.Sprintf("sqlboiler-sqlite3-%d.sql", rand.Int()))
	createSQLiteDB(t, tmpName, b)
	t.Log("sqlite file:", tmpName)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relName, err := filepath.Rel(cwd, tmpName)
	if err != nil {
		t.Fatal(err)
	}

	localName := fmt.Sprintf("./sqlboiler-sqlite3-local-%d.sql", rand.Int())
	createSQLiteDB(t, localName, b)
	t.Cleanup(func() { os.Remove(localName) })

	tests := []struct {
		name       string
		config     drivers.Config
		goldenJson string
	}{
		{
			name: "absolute_path",
			config: drivers.Config{
				"dbname": tmpName,
			},
			goldenJson: "sqlite3.golden.json",
		},
		{
			name: "relative_path",
			config: drivers.Config{
				"dbname": relName,
			},
			goldenJson: "sqlite3.golden.json",
		},
		{
			name: "relative_path_dot_slash",
			config: drivers.Config{
				"dbname": localName,
			},
			goldenJson: "sqlite3.golden.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SQLiteDriver{}
			info, err := s.Assemble(tt.config)
			if err != nil {
				t.Fatal(err)
			}

			got, err := json.MarshalIndent(info, "", "\t")
			if err != nil {
				t.Fatal(err)
			}

			if *flagOverwriteGolden {
				if err = os.WriteFile(tt.goldenJson, got, 0664); err != nil {
					t.Fatal(err)
				}
				t.Log("wrote:", string(got))
				return
			}

			want, err := os.ReadFile(tt.goldenJson)
			if err != nil {
				t.Fatal(err)
			}

			require.JSONEq(t, string(want), string(got))
		})
	}

}
