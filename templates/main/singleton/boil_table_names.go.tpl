var TableNames = struct {
	{{range $table := .Tables}}{{if not $table.IsView -}}
	{{titleCase $table.Name}} string
	{{end}}{{end -}}
}{
	{{range $table := .Tables}}{{if not $table.IsView -}}
	{{titleCase $table.Name}}: "{{$table.Name | $.SchemaTable}}",
	{{end}}{{end -}}
}
