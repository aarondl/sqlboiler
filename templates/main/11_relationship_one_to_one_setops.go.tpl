{{- if or .Table.IsJoinTable .Table.IsView -}}
{{- else -}}
	{{- range $rel := .Table.ToOneRelationships -}}
		{{- $ltable := $.Aliases.Table $rel.Table -}}
		{{- $ftable := $.Aliases.Table $rel.ForeignTable -}}
		{{- $relAlias := $ftable.Relationship $rel.Name -}}
		{{- $col := $ltable.Column $rel.Column -}}
		{{- $fcol := $ftable.Column $rel.ForeignColumn -}}
		{{- $usesPrimitives := usesPrimitives $.Tables $rel.Table $rel.Column $rel.ForeignTable $rel.ForeignColumn -}}
		{{- $schemaForeignTable := $rel.ForeignTable | $.SchemaTable -}}
		{{- $foreignPKeyCols := (getTable $.Tables .ForeignTable).PKey.Columns }}
{{if $.AddGlobal -}}
// Set{{$relAlias.Local}}G of the {{$ltable.DownSingular}} to the related item.
// Sets o.R.{{$relAlias.Local}} to related.
{{- if not $.NoBackReferencing}}
// Adds o to related.R.{{$relAlias.Foreign}}.
{{- end}}
// Uses the global database handle.
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}G({{if not $.NoContext}}ctx context.Context, {{end -}} insert bool, related *{{$ftable.UpSingular}}) error {
	return o.Set{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, insert, related)
}

{{end -}}

{{if $.AddPanic -}}
// Set{{$relAlias.Local}}P of the {{$ltable.DownSingular}} to the related item.
// Sets o.R.{{$relAlias.Local}} to related.
{{- if not $.NoBackReferencing}}
// Adds o to related.R.{{$relAlias.Foreign}}.
{{- end}}
// Panics on error.
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}P({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, insert bool, related *{{$ftable.UpSingular}}) {
	if err := o.Set{{$relAlias.Local}}({{if not $.NoContext}}ctx, {{end -}} exec, insert, related); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

{{if and $.AddGlobal $.AddPanic -}}
// Set{{$relAlias.Local}}GP of the {{$ltable.DownSingular}} to the related item.
// Sets o.R.{{$relAlias.Local}} to related.
{{- if not $.NoBackReferencing}}
// Adds o to related.R.{{$relAlias.Foreign}}.
{{- end}}
// Uses the global database handle and panics on error.
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}GP({{if not $.NoContext}}ctx context.Context, {{end -}} insert bool, related *{{$ftable.UpSingular}}) {
	if err := o.Set{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, insert, related); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

// Set{{$relAlias.Local}} of the {{$ltable.DownSingular}} to the related item.
// Sets o.R.{{$relAlias.Local}} to related.
{{- if not $.NoBackReferencing}}
// Adds o to related.R.{{$relAlias.Foreign}}.
{{- end}}
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, insert bool, related *{{$ftable.UpSingular}}) error {
	
	var err error

	if insert {
		{{if $usesPrimitives -}}
		related.{{$fcol}} = o.{{$col}}
		{{else -}}
		queries.Assign(&related.{{$fcol}}, o.{{$col}})
		{{- end}}

		if err = related.Insert({{if not $.NoContext}}ctx, {{end -}} exec, boil.Infer()); err != nil {
			return errors.Wrap(err, "failed to insert into foreign table")
		}
	} else {
		{{if not $.NoHooks -}}
		if err = o.doBeforeUpdateHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
			return errors.Wrap(err, "failed to perform before update hooks")
		}
		{{end -}}
		updateQuery := fmt.Sprintf(
			"UPDATE {{$schemaForeignTable}} SET %s WHERE %s",
			strmangle.SetParamNames("{{$.LQ}}", "{{$.RQ}}", {{if $.Dialect.UseIndexPlaceholders}}1{{else}}0{{end}}, []string{{"{"}}"{{.ForeignColumn}}"{{"}"}}),
			strmangle.WhereClause("{{$.LQ}}", "{{$.RQ}}", {{if $.Dialect.UseIndexPlaceholders}}2{{else}}0{{end}}, {{$ftable.DownSingular}}PrimaryKeyColumns),
		)
		values := []interface{}{o.{{$col}}, related.{{$foreignPKeyCols | stringMap (aliasCols $ftable) | join ", related."}}{{"}"}}

		{{if $.NoContext -}}
		if boil.DebugMode {
			fmt.Fprintln(boil.DebugWriter, updateQuery)
			fmt.Fprintln(boil.DebugWriter, values)
		}
		{{else -}}
		if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
			fmt.Fprintln(writer, updateQuery)
			fmt.Fprintln(writer, values)
		}
		{{end -}}

		{{if $.NoContext -}}
		if _, err = exec.Exec(updateQuery, values...); err != nil {
		{{else -}}
		if _, err = exec.ExecContext(ctx, updateQuery, values...); err != nil {
		{{end -}}
			return errors.Wrap(err, "failed to update foreign table")
		}

		{{if $usesPrimitives -}}
		related.{{$fcol}} = o.{{$col}}
		{{- else -}}
		queries.Assign(&related.{{$fcol}}, o.{{$col}})
		{{- end}}

		{{if not $.NoHooks -}}
		if err := o.doAfterUpdateHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
			return errors.Wrap(err, "failed to perform after update hooks")
		}
		{{- end}}
	}


	if o.R == nil {
		o.R = &{{$ltable.DownSingular}}R{
			{{$relAlias.Local}}: related,
		}
	} else {
		o.R.{{$relAlias.Local}} = related
	}

	{{if not $.NoBackReferencing -}}
	if related.R == nil {
		related.R = &{{$ftable.DownSingular}}R{
			{{$relAlias.Foreign}}: o,
		}
	} else {
		related.R.{{$relAlias.Foreign}} = o
	}
	{{end -}}

	return nil
}

{{- if .ForeignColumnNullable}}
{{if $.AddGlobal -}}
// Remove{{$relAlias.Local}}G relationship.
// Sets o.R.{{$relAlias.Local}} to nil.
{{- if not $.NoBackReferencing}}
// Removes o from all passed in related items' relationships struct.
{{- end}}
// Uses the global database handle.
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}G({{if not $.NoContext}}ctx context.Context, {{end -}} related *{{$ftable.UpSingular}}) error {
	return o.Remove{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, related)
}

{{end -}}

{{if $.AddPanic -}}
// Remove{{$relAlias.Local}}P relationship.
// Sets o.R.{{$relAlias.Local}} to nil.
{{- if not $.NoBackReferencing}}
// Removes o from all passed in related items' relationships struct.
{{- end}}
// Panics on error.
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}P({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, related *{{$ftable.UpSingular}}) {
	if err := o.Remove{{$relAlias.Local}}({{if not $.NoContext}}ctx, {{end -}} exec, related); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

{{if and $.AddGlobal $.AddPanic -}}
// Remove{{$relAlias.Local}}GP relationship.
// Sets o.R.{{$relAlias.Local}} to nil.
{{- if not $.NoBackReferencing}}
// Removes o from all passed in related items' relationships struct.
{{- end}}
// Uses the global database handle and panics on error.
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}GP({{if not $.NoContext}}ctx context.Context, {{end -}} related *{{$ftable.UpSingular}}) {
	if err := o.Remove{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, related); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

// Remove{{$relAlias.Local}} relationship.
// Sets o.R.{{$relAlias.Local}} to nil.
{{- if not $.NoBackReferencing}}
// Removes o from all passed in related items' relationships struct.
{{- end}}
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, related *{{$ftable.UpSingular}}) error {
	var err error

	queries.SetScanner(&related.{{$fcol}}, nil)
	if {{if not $.NoRowsAffected}}_, {{end -}} err = related.Update({{if not $.NoContext}}ctx, {{end -}} exec, boil.Whitelist("{{.ForeignColumn}}")); err != nil {
		return errors.Wrap(err, "failed to update local table")
	}

	if o.R != nil {
		o.R.{{$relAlias.Local}} = nil
	}

	{{if not $.NoBackReferencing -}}
	if related == nil || related.R == nil {
		return nil
	}

	related.R.{{$relAlias.Foreign}} = nil
	{{- end}}

	return nil
}
{{end -}}{{/* if foreignkey nullable */}}
{{- end -}}{{/* range */}}
{{- end -}}{{/* join table */}}
