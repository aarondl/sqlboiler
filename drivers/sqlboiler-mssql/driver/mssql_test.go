// These tests assume there is a user sqlboiler_driver_user and a database
// by the name of sqlboiler_driver_test that it has full R/W rights to.
// In order to create this you can use the following steps from a root
// mssql account:
//
//   create database sqlboiler_driver_test;
//   go
//   use sqlboiler_driver_test;
//   go
//   create user sqlboiler_driver_user with password = 'sqlboiler';
//   go
//   exec sp_configure 'contained database authentication', 1;
//   go
//   reconfigure
//   go
//   alter database sqlboiler_driver_test set containment = partial;
//   go
//   create user sqlboiler_driver_user with password = 'Sqlboiler@1234';
//   go
//   grant alter, control to sqlboiler_driver_user;
//   go

package driver

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aarondl/sqlboiler/v4/drivers"
)

var (
	flagOverwriteGolden = flag.Bool("overwrite-golden", false, "Overwrite the golden file with the current execution results")

	envHostname = drivers.DefaultEnv("DRIVER_HOSTNAME", "localhost")
	envPort     = drivers.DefaultEnv("DRIVER_PORT", "1433")
	envUsername = drivers.DefaultEnv("DRIVER_USER", "sqlboiler_driver_user")
	envPassword = drivers.DefaultEnv("DRIVER_PASS", "Sqlboiler@1234")
	envDatabase = drivers.DefaultEnv("DRIVER_DB", "sqlboiler_driver_test")

	rgxKeyIDs = regexp.MustCompile(`__[A-F0-9]+$`)
)

func TestDriver(t *testing.T) {
	out := &bytes.Buffer{}
	createDB := exec.Command("sqlcmd", "-S", envHostname, "-U", envUsername, "-P", envPassword, "-d", envDatabase, "-b", "-i", "testdatabase.sql")
	createDB.Stdout = out
	createDB.Stderr = out

	if err := createDB.Run(); err != nil {
		t.Logf("mssql output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
	t.Logf("mssql output:\n%s\n", out.Bytes())

	config := drivers.Config{
		"user":    envUsername,
		"pass":    envPassword,
		"dbname":  envDatabase,
		"host":    envHostname,
		"port":    envPort,
		"sslmode": "disable",
		"schema":  "dbo",
	}

	p := &MSSQLDriver{}
	info, err := p.Assemble(config)
	if err != nil {
		t.Fatal(err)
	}

	for _, t := range info.Tables {
		if t.IsView {
			continue
		}

		t.PKey.Name = rgxKeyIDs.ReplaceAllString(t.PKey.Name, "")
		for i := range t.FKeys {
			t.FKeys[i].Name = rgxKeyIDs.ReplaceAllString(t.FKeys[i].Name, "")
		}
	}

	got, err := json.MarshalIndent(info, "", "\t")
	if err != nil {
		t.Fatal(err)
	}

	if *flagOverwriteGolden {
		if err = os.WriteFile("mssql.golden.json", got, 0664); err != nil {
			t.Fatal(err)
		}
		t.Log("wrote:", string(got))
		return
	}

	want, err := os.ReadFile("mssql.golden.json")
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(want, got) != 0 {
		t.Errorf("want:\n%s\ngot:\n%s\n", want, got)
	}

	t.Run("whitelist table, blacklist column", func(t *testing.T) {
		p := &MSSQLDriver{}
		config := drivers.Config{
			"user":    envUsername,
			"pass":    envPassword,
			"dbname":  envDatabase,
			"host":    envHostname,
			"port":    envPort,
			"sslmode": "disable",
			"schema":  "dbo",
			"whitelist": []string{"magic"},
			"blacklist": []string{"magic.string_three"},
		}
		info, err := p.Assemble(config)
		if err != nil {
			t.Fatal(err)
		}
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
		if found {
			t.Errorf("blacklisted column 'string_three' should not be present in table 'magic'")
		}
	})
}

// TestTableNames_RowsErr verifies that TableNames checks rows.Err() after
// iterating and propagates any error encountered during row iteration back
// to the caller rather than silently returning partial results.
func TestTableNames_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	simulatedErr := fmt.Errorf("rows iteration error")

	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		RowError(0, simulatedErr)

	mock.ExpectQuery(`SELECT table_name`).
		WithArgs("dbo").
		WillReturnRows(rows)

	driver := &MSSQLDriver{conn: db}
	_, err = driver.TableNames("dbo", nil, nil)
	if err == nil {
		t.Fatal("expected error from rows.Err(), got nil")
	}
	if err.Error() != simulatedErr.Error() {
		t.Errorf("expected error %q, got %q", simulatedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

// TestViewNames_RowsErr verifies that ViewNames checks rows.Err() after
// iterating and propagates any error encountered during row iteration back
// to the caller rather than silently returning partial results.
func TestViewNames_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	simulatedErr := fmt.Errorf("rows iteration error")

	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("view1").
		RowError(0, simulatedErr)

	mock.ExpectQuery(`select table_name`).
		WithArgs("dbo").
		WillReturnRows(rows)

	driver := &MSSQLDriver{conn: db}
	_, err = driver.ViewNames("dbo", nil, nil)
	if err == nil {
		t.Fatal("expected error from rows.Err(), got nil")
	}
	if err.Error() != simulatedErr.Error() {
		t.Errorf("expected error %q, got %q", simulatedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}

// TestColumns_RowsErr verifies that Columns checks rows.Err() after iterating
// and propagates any error encountered during row iteration back to the caller
// rather than silently returning partial results.
func TestColumns_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	simulatedErr := fmt.Errorf("rows iteration error")

	rows := sqlmock.NewRows([]string{
		"column_name", "full_type", "data_type", "column_default",
		"is_nullable", "is_unique", "is_identity", "is_computed",
	}).
		AddRow("id", "int", "int", nil, false, true, true, false).
		RowError(0, simulatedErr)

	mock.ExpectQuery(`SELECT column_name`).
		WithArgs("dbo", "test_table").
		WillReturnRows(rows)

	driver := &MSSQLDriver{conn: db}
	_, err = driver.Columns("dbo", "test_table", nil, nil)
	if err == nil {
		t.Fatal("expected error from rows.Err(), got nil")
	}
	if err.Error() != simulatedErr.Error() {
		t.Errorf("expected error %q, got %q", simulatedErr, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %s", err)
	}
}
