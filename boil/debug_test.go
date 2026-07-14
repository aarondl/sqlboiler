package boil

import (
	"testing"
)

func TestSubstituteQueryArgs(t *testing.T) {
	tests := []struct {
		query string
		args  []interface{}
		want  string
	}{
		{
			query: `INSERT INTO "my_table" ("id","name","age","height","is_active","data") VALUES ($1,$2,$3,$4,$5,$6)`,
			args: []interface{}{
				1, "Alice", 25, 1.68, true, []byte(`{"key":"value"}`),
			},
			want: `INSERT INTO "my_table" ("id","name","age","height","is_active","data") VALUES (1,'Alice',25,1.68,true,'{"key":"value"}')`,
		},
		{
			query: `SELECT * FROM "my_table"`,
			args:  []interface{}{},
			want:  `SELECT * FROM "my_table"`,
		},
		{
			query: `UPDATE "my_table" SET "name"=$1,"age"=$2,"height"=$3,"is_active"=$4,"data"=$5 WHERE "id"=$6`,
			args: []interface{}{
				"Bob", 30, 1.78, false, []byte(`{"key":123}`), 2,
			},
			want: `UPDATE "my_table" SET "name"='Bob',"age"=30,"height"=1.78,"is_active"=false,"data"='{"key":123}' WHERE "id"=2`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := substituteQueryArgs(tt.query, tt.args...)

			if got != tt.want {
				t.Errorf("substituteQueryArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}
