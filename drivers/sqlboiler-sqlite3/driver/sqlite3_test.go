package driver

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var (
	flagOverwriteGolden = flag.Bool("overwrite-golden", false, "Overwrite the golden file with the current execution results")
)

func TestDriver(t *testing.T) {
	rand.New(rand.NewSource(time.Now().Unix()))
	b, err := os.ReadFile("testdatabase.sql")
	if err != nil {
		t.Fatal(err)
	}

	tmpName := filepath.Join(os.TempDir(), fmt.Sprintf("sqlboiler-sqlite3-%d.sql", rand.Int()))

	out := &bytes.Buffer{}
	createDB := exec.Command("sqlite3", tmpName)
	createDB.Stdout = out
	createDB.Stderr = out
	createDB.Stdin = bytes.NewReader(b)

	t.Log("sqlite file:", tmpName)
	if err := createDB.Run(); err != nil {
		t.Logf("sqlite output:\n%s\n", out.Bytes())
		t.Fatal(err)
	}
	t.Logf("sqlite output:\n%s\n", out.Bytes())

	tests := []struct {
		name       string
		config     drivers.Config
		goldenJson string
	}{
		{
			name: "default",
			config: drivers.Config{
				"dbname": tmpName,
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

// TestTableNames_RowsErr verifies that TableNames propagates errors surfaced
// by rows.Err() after iterating the result set from sqlite_master. Without the
// rows.Err() check the function would silently return partial results when the
// row iteration is interrupted by a transient error (e.g. a dropped connection).
func TestTableNames_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"name"}).
		AddRow(driver.Value("users")).
		RowError(0, rowErr)
	mock.ExpectQuery(`SELECT name FROM sqlite_master`).WillReturnRows(rows)

	s := SQLiteDriver{dbConn: db}
	_, err = s.TableNames("", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection reset by peer")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestViewNames_RowsErr verifies that ViewNames propagates errors surfaced
// by rows.Err() after iterating the result set from sqlite_master. This is the
// same class of bug as TableNames: without the check, a mid-iteration error
// would be swallowed and the caller would receive a truncated list of views.
func TestViewNames_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"name"}).
		AddRow(driver.Value("user_view")).
		RowError(0, rowErr)
	mock.ExpectQuery(`SELECT name FROM sqlite_master`).WillReturnRows(rows)

	s := SQLiteDriver{dbConn: db}
	_, err = s.ViewNames("", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection reset by peer")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestTableInfo_RowsErr verifies that tableInfo propagates errors surfaced by
// rows.Err() after iterating the PRAGMA table_xinfo result set. A missing
// check here would cause the driver to silently return incomplete column
// metadata for a table, leading to incorrect code generation downstream.
func TestTableInfo_RowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"}).
		AddRow(driver.Value(int64(0)), driver.Value("id"), driver.Value("INTEGER"), driver.Value(true), nil, driver.Value(int64(1)), driver.Value(int64(0))).
		RowError(0, rowErr)
	mock.ExpectQuery(`PRAGMA table_xinfo`).WillReturnRows(rows)

	s := SQLiteDriver{dbConn: db}
	_, err = s.tableInfo("users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection reset by peer")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestIndexes_OuterRowsErr verifies that the indexes function propagates
// errors surfaced by rows.Err() on the outer PRAGMA index_list loop. The outer
// loop enumerates all indexes for a table; an undetected iteration error would
// cause the driver to return an incomplete set of indexes, which could result
// in incorrect uniqueness metadata on generated columns.
//
// We add two rows and set RowError on row 1 so the first iteration completes
// (triggering the inner PRAGMA index_info query) while the second iteration
// fails, making the error available via rows.Err().
func TestIndexes_OuterRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rowErr := fmt.Errorf("connection reset by peer")
	rows := sqlmock.NewRows([]string{"seq", "name", "unique", "origin", "partial"}).
		AddRow(driver.Value(int64(0)), driver.Value("idx_users_email"), driver.Value(int64(1)), driver.Value("c"), driver.Value(int64(0))).
		AddRow(driver.Value(int64(1)), driver.Value("idx_users_name"), driver.Value(int64(0)), driver.Value("c"), driver.Value(int64(0))).
		RowError(1, rowErr)
	mock.ExpectQuery(`PRAGMA index_list`).WillReturnRows(rows)

	// The first outer iteration succeeds, so the inner query for index_info
	// is issued for the first index.
	innerRows := sqlmock.NewRows([]string{"seqno", "cid", "name"}).
		AddRow(driver.Value(int64(0)), driver.Value(int64(1)), driver.Value("email"))
	mock.ExpectQuery(`PRAGMA index_info`).WillReturnRows(innerRows)

	s := SQLiteDriver{dbConn: db}
	_, err = s.indexes("users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection reset by peer")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestIndexes_InnerRowsErr verifies that the indexes function propagates
// errors surfaced by rowsColumns.Err() on the inner PRAGMA index_info loop.
// The inner loop retrieves the columns belonging to a specific index; a
// swallowed error here would silently produce an index with missing columns,
// corrupting the uniqueness analysis that depends on column counts.
func TestIndexes_InnerRowsErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	outerRows := sqlmock.NewRows([]string{"seq", "name", "unique", "origin", "partial"}).
		AddRow(driver.Value(int64(0)), driver.Value("idx_users_email"), driver.Value(int64(1)), driver.Value("c"), driver.Value(int64(0)))
	mock.ExpectQuery(`PRAGMA index_list`).WillReturnRows(outerRows)

	rowErr := fmt.Errorf("connection reset by peer")
	innerRows := sqlmock.NewRows([]string{"seqno", "cid", "name"}).
		AddRow(driver.Value(int64(0)), driver.Value(int64(1)), driver.Value("email")).
		RowError(0, rowErr)
	mock.ExpectQuery(`PRAGMA index_info`).WillReturnRows(innerRows)

	s := SQLiteDriver{dbConn: db}
	_, err = s.indexes("users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection reset by peer")
	require.NoError(t, mock.ExpectationsWereMet())
}
