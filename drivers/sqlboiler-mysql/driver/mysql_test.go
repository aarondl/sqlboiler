// These tests assume there is a user sqlboiler_test_user and a database
// by the name of sqlboiler_test that it has full R/W rights to.
// In order to create this you can use the following steps from a root
// mysql account:
//
//   create user sqlboiler_driver_user identified by 'sqlboiler';
//   create database sqlboiler_driver_test;
//   grant all privileges on sqlboiler_driver_test.* to sqlboiler_driver_user;

package driver

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/aarondl/sqlboiler/v4/drivers"
)

var (
	flagOverwriteGolden = flag.Bool("overwrite-golden", false, "Overwrite the golden file with the current execution results")

	envHostname = drivers.DefaultEnv("DRIVER_HOSTNAME", "localhost")
	envPort     = drivers.DefaultEnv("DRIVER_PORT", "3306")
	envUsername = drivers.DefaultEnv("DRIVER_USER", "sqlboiler_driver_user")
	envPassword = drivers.DefaultEnv("DRIVER_PASS", "sqlboiler")
	envDatabase = drivers.DefaultEnv("DRIVER_DB", "sqlboiler_driver_test")
)

func TestDriver(t *testing.T) {
	b, err := os.ReadFile("testdatabase.sql")
	if err != nil {
		t.Fatal(err)
	}

	out := &bytes.Buffer{}
	createDB := exec.Command("mysql", "-h", envHostname, "-P", envPort, "-u", envUsername, fmt.Sprintf("-p%s", envPassword), envDatabase)
	createDB.Stdout = out
	createDB.Stderr = out
	createDB.Stdin = bytes.NewReader(b)

	if err := createDB.Run(); err != nil {
		t.Logf("mysql output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
	t.Logf("mysql output:\n%s\n", out.Bytes())

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
				"sslmode": "false",
				"schema":  envDatabase,
			},
			goldenJson: "mysql.golden.json",
		},
		{
			name: "enum_types",
			config: drivers.Config{
				"user":           envUsername,
				"pass":           envPassword,
				"dbname":         envDatabase,
				"host":           envHostname,
				"port":           envPort,
				"sslmode":        "false",
				"schema":         envDatabase,
				"add-enum-types": true,
			},
			goldenJson: "mysql.golden.enums.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &MySQLDriver{}
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
		p := &MySQLDriver{}
		config := drivers.Config{
			"user":    envUsername,
			"pass":    envPassword,
			"dbname":  envDatabase,
			"host":    envHostname,
			"port":    envPort,
			"sslmode": "false",
			"schema":  envDatabase,
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

// TestTableNames_RowsErr verifies that TableNames propagates errors returned
// by rows.Err() after the row-iteration loop. A RowError on the first row
// simulates a mid-iteration failure (e.g. a dropped connection) and the test
// asserts that the caller receives the underlying error instead of a nil.
func TestTableNames_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	simulatedErr := fmt.Errorf("simulated row iteration error")

	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		RowError(0, simulatedErr)

	mock.ExpectQuery(`select table_name from information_schema\.tables`).
		WithArgs("testschema").
		WillReturnRows(rows)

	m := &MySQLDriver{conn: db}
	_, err = m.TableNames("testschema", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated row iteration error")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestViewNames_RowsErr verifies that ViewNames propagates errors returned
// by rows.Err() after the row-iteration loop. A RowError on the first row
// simulates a mid-iteration failure and the test asserts that the caller
// receives the underlying error instead of a nil.
func TestViewNames_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	simulatedErr := fmt.Errorf("simulated row iteration error")

	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("view1").
		RowError(0, simulatedErr)

	mock.ExpectQuery(`select table_name from information_schema\.views`).
		WithArgs("testschema").
		WillReturnRows(rows)

	m := &MySQLDriver{conn: db}
	_, err = m.ViewNames("testschema", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated row iteration error")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestColumns_RowsErr verifies that Columns propagates errors returned by
// rows.Err() after the row-iteration loop. A RowError on the first row
// simulates a mid-iteration failure and the test asserts that the caller
// receives the underlying error instead of a nil.
func TestColumns_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	simulatedErr := fmt.Errorf("simulated row iteration error")

	rows := sqlmock.NewRows([]string{
		"column_name", "column_type", "column_comment", "data_type",
		"column_default", "is_nullable", "is_generated", "is_unique",
	}).
		AddRow("id", "int", "", "int", nil, false, false, true).
		RowError(0, simulatedErr)

	mock.ExpectQuery(`select\s+c\.column_name`).
		WithArgs("test_table", "test_table", "testschema", "testschema", "testschema", "testschema", "test_table", "test_table", "testschema").
		WillReturnRows(rows)

	m := &MySQLDriver{conn: db}
	_, err = m.Columns("testschema", "test_table", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated row iteration error")
	require.NoError(t, mock.ExpectationsWereMet())
}
