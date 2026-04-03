// These tests assume there is a user sqlboiler_driver_user and a database
// by the name of sqlboiler_driver_test that it has full R/W rights to.
// In order to create this you can use the following steps from a root
// psql account:
//
//   create role sqlboiler_driver_user login nocreatedb nocreaterole nocreateuser password 'sqlboiler';
//   create database sqlboiler_driver_test owner = sqlboiler_driver_user;

package driver

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/aarondl/sqlboiler/v4/drivers"
)

var (
	flagOverwriteGolden = flag.Bool("overwrite-golden", false, "Overwrite the golden file with the current execution results")

	envHostname = drivers.DefaultEnv("DRIVER_HOSTNAME", "localhost")
	envPort     = drivers.DefaultEnv("DRIVER_PORT", "5432")
	envUsername = drivers.DefaultEnv("DRIVER_USER", "sqlboiler_driver_user")
	envPassword = drivers.DefaultEnv("DRIVER_PASS", "sqlboiler")
	envDatabase = drivers.DefaultEnv("DRIVER_DB", "sqlboiler_driver_test")
)

func TestAssemble(t *testing.T) {
	b, err := os.ReadFile("testdatabase.sql")
	if err != nil {
		t.Fatal(err)
	}

	out := &bytes.Buffer{}
	createDB := exec.Command("psql", "-h", envHostname, "-U", envUsername, envDatabase)
	createDB.Env = append([]string{fmt.Sprintf("PGPASSWORD=%s", envPassword)}, os.Environ()...)
	createDB.Stdout = out
	createDB.Stderr = out
	createDB.Stdin = bytes.NewReader(b)

	if err := createDB.Run(); err != nil {
		t.Logf("psql output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
	t.Logf("psql output:\n%s\n", out.Bytes())

	tests := []struct {
		name       string
		config     drivers.Config
		goldenJson string
	}{
		{
			name: "default",
			config: drivers.Config{
				"user":    envUsername,
				"pass":    envPassword,
				"dbname":  envDatabase,
				"host":    envHostname,
				"port":    envPort,
				"sslmode": "disable",
				"schema":  "public",
			},
			goldenJson: "psql.golden.json",
		},
		{
			name: "enum_types",
			config: drivers.Config{
				"user":           envUsername,
				"pass":           envPassword,
				"dbname":         envDatabase,
				"host":           envHostname,
				"port":           envPort,
				"sslmode":        "disable",
				"schema":         "public",
				"add-enum-types": true,
			},
			goldenJson: "psql.golden.enums.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PostgresDriver{}
			info, err := p.Assemble(tt.config)
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

	t.Run("whitelist table, blacklist column", func(t *testing.T) {
		p := PostgresDriver{}
		config := drivers.Config{
			"user":    envUsername,
			"pass":    envPassword,
			"dbname":  envDatabase,
			"host":    envHostname,
			"port":    envPort,
			"sslmode": "disable",
			"schema":  "public",
			"whitelist": []string{"magic"},
			"blacklist": []string{"magic.string_three"},
		}
		info, err := p.Assemble(config)
		require.NoError(t, err)
		found := false
		for _, tbl := range info.Tables {
			if tbl.Name == "magic" {
				for _, col := range tbl.Columns {
					if col.Name == "string_three" {
						found = true
					}
				}
			}
		}
		require.False(t, found, "blacklisted column 'string_three' should not be present in table 'magic'")
	})
}

// TestTableNames_RowsErr verifies that TableNames propagates errors surfaced by
// rows.Err() after the iteration loop. Without the rows.Err() check the
// function would silently return a partial (or empty) result set when the
// database connection fails mid-iteration, violating the database/sql contract.
func TestTableNames_RowsErr(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		RowError(0, rowErr)
	mock.ExpectQuery(`select table_name from information_schema\.tables`).WillReturnRows(rows)

	p := &PostgresDriver{conn: db}
	names, err := p.TableNames("public", nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if names != nil {
		t.Errorf("expected nil result, got: %v", names)
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Errorf("error did not contain expected message, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestViewNames_RowsErr verifies that ViewNames propagates errors surfaced by
// rows.Err() after the iteration loop. A mid-iteration failure (e.g. a
// connection reset) must be returned to the caller so it does not act on an
// incomplete list of view names.
func TestViewNames_RowsErr(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("view1").
		RowError(0, rowErr)
	mock.ExpectQuery(`select`).WillReturnRows(rows)

	p := &PostgresDriver{conn: db}
	names, err := p.ViewNames("public", nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if names != nil {
		t.Errorf("expected nil result, got: %v", names)
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Errorf("error did not contain expected message, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestLoadUniqueColumns_RowsErr verifies that loadUniqueColumns propagates
// errors surfaced by rows.Err() after the iteration loop. This function
// populates the driver's uniqueColumns cache; a silent mid-iteration failure
// would leave the cache incomplete, causing downstream Columns() calls to
// produce incorrect uniqueness metadata.
func TestLoadUniqueColumns_RowsErr(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"schema_name", "table_name", "column_name"}).
		AddRow("public", "table1", "id").
		RowError(0, rowErr)
	mock.ExpectQuery(`with`).WillReturnRows(rows)

	p := &PostgresDriver{conn: db}
	err = p.loadUniqueColumns()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Errorf("error did not contain expected message, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestColumns_RowsErr verifies that Columns propagates errors surfaced by
// rows.Err() after the iteration loop. Column metadata drives code generation,
// so a silently truncated result set could produce generated code that is
// missing fields or has wrong types. The test pre-initializes uniqueColumns so
// only the Columns query itself is exercised.
func TestColumns_RowsErr(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{
		"column_name", "column_type", "column_full_type", "udt_name",
		"array_type", "domain_name", "column_default", "column_comment",
		"is_nullable", "is_generated", "is_identity",
	}).
		AddRow("id", "integer", "integer", "int4",
			nil, nil, nil, "",
			false, false, false).
		RowError(0, rowErr)
	mock.ExpectQuery(`SELECT`).WillReturnRows(rows)

	p := &PostgresDriver{
		conn:          db,
		uniqueColumns: &sync.Map{},
	}
	cols, err := p.Columns("public", "test_table", nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if cols != nil {
		t.Errorf("expected nil result, got: %v", cols)
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Errorf("error did not contain expected message, got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
