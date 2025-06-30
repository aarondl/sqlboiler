{{- $alias := .Aliases.Table .Table.Name -}}
{{- $orig_tbl_name := .Table.Name -}}

// {{$alias.UpSingular}} is an object representing the database table.
type {{$alias.UpSingular}} struct {
	{{- range $index, $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{- $orig_col_name := $column.Name -}}
	{{- range $column.Comment | splitLines -}} 
	{{- if eq $index 0 -}}
	{{ "\n" }}
	{{- end -}}
	// {{ . }}
	{{ end -}}

	{{if ignore $orig_tbl_name $orig_col_name $.TagIgnore -}}
	{{$colAlias}} {{$column.Type}} `{{generateIgnoreTags $.Tags}}boil:"{{$column.Name}}" json:"-" toml:"-" yaml:"-"`
	{{ else -}}

	{{- /* render column alias and column type */ -}}
	{{ $colAlias }} {{ $column.Type -}}

	{{- /*
	  handle struct tags
	  StructTagCasing will be replaced with $.StructTagCases
	  however we need to keep this backward compatible
	  $.StructTagCasing will only be used when it's set to "alias"
    */ -}}
	`
	{{- if eq $.StructTagCasing "alias" -}}
	    {{- generateTags $.Tags $colAlias -}}
	    {{- generateTagWithCase "boil" $column.Name $colAlias "alias" false -}}
	    {{- generateTagWithCase "json" $column.Name $colAlias "alias" $column.Nullable -}}
	    {{- generateTagWithCase "toml" $column.Name $colAlias "alias" false -}}
	    {{- trim (generateTagWithCase "yaml" $column.Name $colAlias "alias" $column.Nullable) -}}
	{{- else -}}
	    {{- generateTags $.Tags $column.Name }}
	    {{- generateTagWithCase "boil" $column.Name $colAlias $.StructTagCases.Boil false -}}
	    {{- generateTagWithCase "json" $column.Name $colAlias $.StructTagCases.Json $column.Nullable -}}
	    {{- generateTagWithCase "toml" $column.Name $colAlias $.StructTagCases.Toml false -}}
	    {{- trim (generateTagWithCase "yaml" $column.Name $colAlias $.StructTagCases.Yaml $column.Nullable) -}}
	{{- end -}}
	`
	{{ end -}}
	{{ end -}}

	{{- if or .Table.IsJoinTable .Table.IsView -}}
	{{- else}}
	R *{{$alias.DownSingular}}R `{{generateTags $.Tags $.RelationTag}}boil:"{{$.RelationTag}}" json:"{{$.RelationTag}}" toml:"{{$.RelationTag}}" yaml:"{{$.RelationTag}}"`
	L {{$alias.DownSingular}}L `{{generateIgnoreTags $.Tags}}boil:"-" json:"-" toml:"-" yaml:"-"`
	{{end -}}

	// customTableName is for custom table name insertion
	customTableName string
}

var {{$alias.UpSingular}}Columns = struct {
	{{range $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{$colAlias}} string
	{{end -}}
}{
	{{range $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{$colAlias}}: "{{$column.Name}}",
	{{end -}}
}

var {{$alias.UpSingular}}TableColumns = struct {
	{{range $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{$colAlias}} string
	{{end -}}
}{
	{{range $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{$colAlias}}: "{{$orig_tbl_name}}.{{$column.Name}}",
	{{end -}}
}

{{/* Generated where helpers for all types in the database */}}
// Generated where
{{- range .Table.Columns -}}
	{{- if (oncePut $.DBTypes .Type)}}
		{{$name := printf "whereHelper%s" (goVarname .Type)}}
type {{$name}} struct { field string }
func (w {{$name}}) EQ(x {{.Type}}) qm.QueryMod { return qmhelper.Where{{if .Nullable}}NullEQ(w.field, false, x){{else}}(w.field, qmhelper.EQ, x){{end}} }
func (w {{$name}}) NEQ(x {{.Type}}) qm.QueryMod { return qmhelper.Where{{if .Nullable}}NullEQ(w.field, true, x){{else}}(w.field, qmhelper.NEQ, x){{end}} }
func (w {{$name}}) LT(x {{.Type}}) qm.QueryMod { return qmhelper.Where(w.field, qmhelper.LT, x) }
func (w {{$name}}) LTE(x {{.Type}}) qm.QueryMod { return qmhelper.Where(w.field, qmhelper.LTE, x) }
func (w {{$name}}) GT(x {{.Type}}) qm.QueryMod { return qmhelper.Where(w.field, qmhelper.GT, x) }
func (w {{$name}}) GTE(x {{.Type}}) qm.QueryMod { return qmhelper.Where(w.field, qmhelper.GTE, x) }
		{{if or (eq .Type "string") (eq .Type "null.String") -}}
func (w {{$name}}) LIKE(x {{.Type}}) qm.QueryMod { return qm.Where(w.field+" LIKE ?", x) }
func (w {{$name}}) NLIKE(x {{.Type}}) qm.QueryMod { return qm.Where(w.field+" NOT LIKE ?", x) }
			{{- block "where_ilike_override" . }}{{- end}}
			{{- block "where_similarto_override" . }}{{- end}}
		{{end -}}
		{{if or (isPrimitive .Type) (isNullPrimitive .Type) (isEnumDBType .DBType) -}}
func (w {{$name}}) IN(slice []{{convertNullToPrimitive .Type}}) qm.QueryMod {
	values := make([]interface{}, 0, len(slice))
	for _, value := range slice {
		values = append(values, value)
	}
	return qm.WhereIn(fmt.Sprintf("%s IN ?", w.field), values...)
}
func (w {{$name}}) NIN(slice []{{convertNullToPrimitive .Type}}) qm.QueryMod {
	values := make([]interface{}, 0, len(slice))
	for _, value := range slice {
	  values = append(values, value)
	}
	return qm.WhereNotIn(fmt.Sprintf("%s NOT IN ?", w.field), values...)
}
		{{end -}}
	{{end -}}
	{{if .Nullable -}}
		{{- if (oncePut $.DBTypes (printf "%s.null" .Type))}}
		{{$name := printf "whereHelper%s" (goVarname .Type)}}
func (w {{$name}}) IsNull() qm.QueryMod { return qmhelper.WhereIsNull(w.field) }
func (w {{$name}}) IsNotNull() qm.QueryMod { return qmhelper.WhereIsNotNull(w.field) }
		{{end -}}
	{{end -}}
{{- end}}

var {{$alias.UpSingular}}Where = struct {
	{{range $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{$colAlias}} whereHelper{{goVarname $column.Type}}
	{{end -}}
}{
	{{range $column := .Table.Columns -}}
	{{- $colAlias := $alias.Column $column.Name -}}
	{{$colAlias}}: whereHelper{{goVarname $column.Type}}{field: "{{$.Table.Name | $.SchemaTable}}.{{$column.Name | $.Quotes}}"},
	{{end -}}
}

{{if or .Table.IsJoinTable .Table.IsView -}}
{{- else -}}
// {{$alias.UpSingular}}Rels is where relationship names are stored.
var {{$alias.UpSingular}}Rels = struct {
	{{range .Table.FKeys -}}
	{{- $relAlias := $alias.Relationship .Name -}}
	{{$relAlias.Foreign}} string
	{{end -}}

	{{range .Table.ToOneRelationships -}}
	{{- $ftable := $.Aliases.Table .ForeignTable -}}
	{{- $relAlias := $ftable.Relationship .Name -}}
	{{$relAlias.Local}} string
	{{end -}}

	{{range .Table.ToManyRelationships -}}
	{{- $relAlias := $.Aliases.ManyRelationship .ForeignTable .Name .JoinTable .JoinLocalFKeyName -}}
	{{$relAlias.Local}} string
	{{end -}}{{/* range tomany */}}
}{
	{{range .Table.FKeys -}}
	{{- $relAlias := $alias.Relationship .Name -}}
	{{$relAlias.Foreign}}: "{{$relAlias.Foreign}}",
	{{end -}}

	{{range .Table.ToOneRelationships -}}
	{{- $ftable := $.Aliases.Table .ForeignTable -}}
	{{- $relAlias := $ftable.Relationship .Name -}}
	{{$relAlias.Local}}: "{{$relAlias.Local}}",
	{{end -}}

	{{range .Table.ToManyRelationships -}}
	{{- $relAlias := $.Aliases.ManyRelationship .ForeignTable .Name .JoinTable .JoinLocalFKeyName -}}
	{{$relAlias.Local}}: "{{$relAlias.Local}}",
	{{end -}}{{/* range tomany */}}
}

// {{$alias.DownSingular}}R is where relationships are stored.
type {{$alias.DownSingular}}R struct {
	{{range .Table.FKeys -}}
	{{- $ftable := $.Aliases.Table .ForeignTable -}}
	{{- $relAlias := $alias.Relationship .Name -}}
	{{$relAlias.Foreign}} *{{$ftable.UpSingular}} `{{generateTags $.Tags $relAlias.Foreign}}boil:"{{$relAlias.Foreign}}" json:"{{$relAlias.Foreign}}" toml:"{{$relAlias.Foreign}}" yaml:"{{$relAlias.Foreign}}"`
	{{end -}}

	{{range .Table.ToOneRelationships -}}
	{{- $ftable := $.Aliases.Table .ForeignTable -}}
	{{- $relAlias := $ftable.Relationship .Name -}}
	{{$relAlias.Local}} *{{$ftable.UpSingular}} `{{generateTags $.Tags $relAlias.Local}}boil:"{{$relAlias.Local}}" json:"{{$relAlias.Local}}" toml:"{{$relAlias.Local}}" yaml:"{{$relAlias.Local}}"`
	{{end -}}

	{{range .Table.ToManyRelationships -}}
	{{- $ftable := $.Aliases.Table .ForeignTable -}}
	{{- $relAlias := $.Aliases.ManyRelationship .ForeignTable .Name .JoinTable .JoinLocalFKeyName -}}
	{{$relAlias.Local}} {{printf "%sSlice" $ftable.UpSingular}} `{{generateTags $.Tags $relAlias.Local}}boil:"{{$relAlias.Local}}" json:"{{$relAlias.Local}}" toml:"{{$relAlias.Local}}" yaml:"{{$relAlias.Local}}"`
	{{end -}}{{/* range tomany */}}
}

// NewStruct creates a new relationship struct
func (*{{$alias.DownSingular}}R) NewStruct() *{{$alias.DownSingular}}R {
	return &{{$alias.DownSingular}}R{}
}

{{range .Table.FKeys -}}
{{- $ftable := $.Aliases.Table .ForeignTable -}}
{{- $relAlias := $alias.Relationship .Name -}}

{{- if not $.NoRelationGetters}}

func (o *{{$alias.UpSingular}}) Get{{$relAlias.Foreign}}() *{{$ftable.UpSingular}} {
	if (o == nil) {
		return nil
	}

	return o.R.Get{{$relAlias.Foreign}}()
}

{{end -}}

func (r *{{$alias.DownSingular}}R) Get{{$relAlias.Foreign}}() *{{$ftable.UpSingular}} {
	if (r == nil) {
		return nil
	}

	return r.{{$relAlias.Foreign}}
}

{{end -}}

{{- range .Table.ToOneRelationships -}}
{{- $ftable := $.Aliases.Table .ForeignTable -}}
{{- $relAlias := $ftable.Relationship .Name -}}

{{- if not $.NoRelationGetters}}

func (o *{{$alias.UpSingular}}) Get{{$relAlias.Local}}() *{{$ftable.UpSingular}} {
	if (o == nil) {
		return nil
	}

	return o.R.Get{{$relAlias.Local}}()
}

{{end -}}

func (r *{{$alias.DownSingular}}R) Get{{$relAlias.Local}}() *{{$ftable.UpSingular}} {
	if (r == nil) {
		return nil
	}

	return r.{{$relAlias.Local}}
}

{{end -}}

{{- range .Table.ToManyRelationships -}}
{{- $ftable := $.Aliases.Table .ForeignTable -}}
{{- $relAlias := $.Aliases.ManyRelationship .ForeignTable .Name .JoinTable .JoinLocalFKeyName -}}

{{- if not $.NoRelationGetters}}

func (o *{{$alias.UpSingular}}) Get{{$relAlias.Local}}() {{printf "%sSlice" $ftable.UpSingular}} {
	if (o == nil) {
		return nil
	}

	return o.R.Get{{$relAlias.Local}}()
}

{{end -}}

func (r *{{$alias.DownSingular}}R) Get{{$relAlias.Local}}() {{printf "%sSlice" $ftable.UpSingular}} {
	if (r == nil) {
		return nil
	}

	return r.{{$relAlias.Local}}
}

{{end -}}

// {{$alias.DownSingular}}L is where Load methods for each relationship are stored.
type {{$alias.DownSingular}}L struct{}
{{end -}}
