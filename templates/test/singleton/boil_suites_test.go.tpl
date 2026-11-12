// This test suite runs each operation test in parallel.
// Example, if your database has 3 tables, the suite will run:
// table1, table2 and table3 Delete in parallel
// table1, table2 and table3 Insert in parallel, and so forth.
// It does NOT run each operation group in parallel.
// Separating the tests thusly grants avoidance of Postgres deadlocks.
func TestParent(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}})
  {{end -}}
  {{- end -}}
}

{{if .AddSoftDeletes -}}
func TestSoftDelete(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  	{{- if .CanSoftDelete $.AutoColumns.Deleted -}}
      {{- $alias := $.Aliases.Table .Name -}}
      t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}SoftDelete)
  	{{end -}}
  {{end -}}
  {{- end -}}
}

func TestQuerySoftDeleteAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  	{{- if .CanSoftDelete $.AutoColumns.Deleted -}}
      {{- $alias := $.Aliases.Table .Name -}}
      t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}QuerySoftDeleteAll)
  	{{end -}}
  {{end -}}
  {{- end -}}
}

func TestSliceSoftDeleteAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  	{{- if .CanSoftDelete $.AutoColumns.Deleted -}}
      {{- $alias := $.Aliases.Table .Name -}}
      t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}SliceSoftDeleteAll)
  	{{end -}}
  {{end -}}
  {{- end -}}
}
{{- end}}

func TestDelete(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Delete)
  {{end -}}
  {{- end -}}
}

func TestQueryDeleteAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}QueryDeleteAll)
  {{end -}}
  {{- end -}}
}

func TestSliceDeleteAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}SliceDeleteAll)
  {{end -}}
  {{- end -}}
}

func TestExists(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Exists)
  {{end -}}
  {{- end -}}
}

func TestFind(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Find)
  {{end -}}
  {{- end -}}
}

func TestBind(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Bind)
  {{end -}}
  {{- end -}}
}

func TestOne(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}One)
  {{end -}}
  {{- end -}}
}

func TestAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}All)
  {{end -}}
  {{- end -}}
}

func TestCount(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Count)
  {{end -}}
  {{- end -}}
}

func TestCountDistinct(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}CountDistinct)
  {{end -}}
  {{- end -}}
}

{{if not .NoHooks -}}
func TestHooks(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Hooks)
  {{end -}}
  {{- end -}}
}
{{- end}}

func TestInsert(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Insert)
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}InsertWhitelist)
  {{end -}}
  {{- end -}}
}

func TestReload(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Reload)
  {{end -}}
  {{- end -}}
}

func TestReloadAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}ReloadAll)
  {{end -}}
  {{- end -}}
}

func TestSelect(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Select)
  {{end -}}
  {{- end -}}
}

func TestUpdate(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}Update)
  {{end -}}
  {{- end -}}
}

func TestSliceUpdateAll(t *testing.T) {
  {{- range .Tables}}
  {{- if or .IsJoinTable .IsView -}}
  {{- else -}}
  {{- $alias := $.Aliases.Table .Name -}}
  t.Run("{{$alias.UpPlural}}", test{{$alias.UpPlural}}SliceUpdateAll)
  {{end -}}
  {{- end -}}
}
