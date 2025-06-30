{{- if or .Table.IsJoinTable .Table.IsView -}}
{{- else -}}
	{{- $table := .Table -}}
	{{- range $rel := .Table.ToManyRelationships -}}
		{{- $ltable := $.Aliases.Table $rel.Table -}}
		{{- $ftable := $.Aliases.Table $rel.ForeignTable -}}
		{{- $relAlias := $.Aliases.ManyRelationship $rel.ForeignTable $rel.Name $rel.JoinTable $rel.JoinLocalFKeyName -}}
		{{- $col := $ltable.Column $rel.Column -}}
		{{- $fcol := $ftable.Column $rel.ForeignColumn -}}
		{{- $usesPrimitives := usesPrimitives $.Tables $rel.Table $rel.Column $rel.ForeignTable $rel.ForeignColumn -}}
		{{- $schemaForeignTable := $rel.ForeignTable | $.SchemaTable }}
		{{- $foreignPKeyCols := (getTable $.Tables $rel.ForeignTable).PKey.Columns }}
{{if $.AddGlobal -}}
// Add{{$relAlias.Local}}G adds the given related objects to the existing relationships
// of the {{$table.Name | singular}}, optionally inserting them as new records.
// Appends related to o.R.{{$relAlias.Local}}.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}} appropriately.
{{- end}}
// Uses the global database handle.
func (o *{{$ltable.UpSingular}}) Add{{$relAlias.Local}}G({{if not $.NoContext}}ctx context.Context, {{end -}} insert bool, related ...*{{$ftable.UpSingular}}) error {
	return o.Add{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, insert, related...)
}

{{end -}}

{{if $.AddPanic -}}
// Add{{$relAlias.Local}}P adds the given related objects to the existing relationships
// of the {{$table.Name | singular}}, optionally inserting them as new records.
// Appends related to o.R.{{$relAlias.Local}}.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}} appropriately.
{{- end}}
// Panics on error.
func (o *{{$ltable.UpSingular}}) Add{{$relAlias.Local}}P({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, insert bool, related ...*{{$ftable.UpSingular}}) {
	if err := o.Add{{$relAlias.Local}}({{if not $.NoContext}}ctx, {{end -}} exec, insert, related...); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

{{if and $.AddGlobal $.AddPanic -}}
// Add{{$relAlias.Local}}GP adds the given related objects to the existing relationships
// of the {{$table.Name | singular}}, optionally inserting them as new records.
// Appends related to o.R.{{$relAlias.Local}}.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}} appropriately.
{{- end}}
// Uses the global database handle and panics on error.
func (o *{{$ltable.UpSingular}}) Add{{$relAlias.Local}}GP({{if not $.NoContext}}ctx context.Context, {{end -}} insert bool, related ...*{{$ftable.UpSingular}}) {
	if err := o.Add{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, insert, related...); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

// Add{{$relAlias.Local}} adds the given related objects to the existing relationships
// of the {{$table.Name | singular}}, optionally inserting them as new records.
// Appends related to o.R.{{$relAlias.Local}}.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}} appropriately.
{{- end}}
func (o *{{$ltable.UpSingular}}) Add{{$relAlias.Local}}({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, insert bool, related ...*{{$ftable.UpSingular}}) error {
	var err error
	
	{{if not $.NoHooks -}}
	if err := o.doBeforeInsertHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
		return err
	}
	{{- end}}
	
	for _, rel := range related {
		if insert {
			{{if not .ToJoinTable -}}
				{{if $usesPrimitives -}}
			rel.{{$fcol}} = o.{{$col}}
				{{else -}}
			queries.Assign(&rel.{{$fcol}}, o.{{$col}})
				{{end -}}
			{{end -}}

			if err = rel.Insert({{if not $.NoContext}}ctx, {{end -}} exec, boil.Infer()); err != nil {
				return errors.Wrap(err, "failed to insert into foreign table")
			}
		}{{if not .ToJoinTable}} else {
			updateQuery := fmt.Sprintf(
				"UPDATE {{$schemaForeignTable}} SET %s WHERE %s",
				strmangle.SetParamNames("{{$.LQ}}", "{{$.RQ}}", {{if $.Dialect.UseIndexPlaceholders}}1{{else}}0{{end}}, []string{{"{"}}"{{.ForeignColumn}}"{{"}"}}),
				strmangle.WhereClause("{{$.LQ}}", "{{$.RQ}}", {{if $.Dialect.UseIndexPlaceholders}}2{{else}}0{{end}}, {{$ftable.DownSingular}}PrimaryKeyColumns),
			)
			values := []interface{}{o.{{$col}}, rel.{{$foreignPKeyCols | stringMap (aliasCols $ftable) | join ", rel."}}{{"}"}}

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
			rel.{{$fcol}} = o.{{$col}}
			{{else -}}
			queries.Assign(&rel.{{$fcol}}, o.{{$col}})
			{{end -}}
		}{{end -}}
	}

	{{if .ToJoinTable -}}
	for _, rel := range related {
		query := "insert into {{.JoinTable | $.SchemaTable}} ({{.JoinLocalColumn | $.Quotes}}, {{.JoinForeignColumn | $.Quotes}}) values {{if $.Dialect.UseIndexPlaceholders}}($1, $2){{else}}(?, ?){{end}}"
		values := []interface{}{{"{"}}o.{{$col}}, rel.{{$fcol}}}

		{{if $.NoContext -}}
		if boil.DebugMode {
			fmt.Fprintln(boil.DebugWriter, query)
			fmt.Fprintln(boil.DebugWriter, values)
		}
		{{else -}}
		if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
			fmt.Fprintln(writer, query)
			fmt.Fprintln(writer, values)
		}
		{{end -}}

		{{if $.NoContext -}}
		_, err = exec.Exec(query, values...)
		{{else -}}
		_, err = exec.ExecContext(ctx, query, values...)
		{{end -}}
		if err != nil {
			return errors.Wrap(err, "failed to insert into join table")
		}
	}
	{{end -}}

	if o.R == nil {
		o.R = &{{$ltable.DownSingular}}R{
			{{$relAlias.Local}}: related,
		}
	} else {
		o.R.{{$relAlias.Local}} = append(o.R.{{$relAlias.Local}}, related...)
	}

	{{if not $.NoBackReferencing -}}
	{{if .ToJoinTable -}}
	for _, rel := range related {
		if rel.R == nil {
			rel.R = &{{$ftable.DownSingular}}R{
				{{$relAlias.Foreign}}: {{$ltable.UpSingular}}Slice{{"{"}}o{{"}"}},
			}
		} else {
			rel.R.{{$relAlias.Foreign}} = append(rel.R.{{$relAlias.Foreign}}, o)
		}
	}
	{{else -}}
	for _, rel := range related {
		if rel.R == nil {
			rel.R = &{{$ftable.DownSingular}}R{
				{{$relAlias.Foreign}}: o,
			}
		} else {
			rel.R.{{$relAlias.Foreign}} = o
		}
	}
	{{end -}}
	{{end -}}

	{{if not $.NoHooks -}}
	return o.doAfterInsertHooks({{if not $.NoContext}}ctx, {{end -}} exec)
	{{- else -}}
	return nil
	{{- end}}
}

			{{- if (or .ForeignColumnNullable .ToJoinTable)}}
{{if $.AddGlobal -}}
// Set{{$relAlias.Local}}G removes all previously related items of the
// {{$table.Name | singular}} replacing them completely with the passed
// in related items, optionally inserting them as new records.
// Sets o.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
// Replaces o.R.{{$relAlias.Local}} with related.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
{{- end}}
// Uses the global database handle.
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}G({{if not $.NoContext}}ctx context.Context, {{end -}} insert bool, related ...*{{$ftable.UpSingular}}) error {
	return o.Set{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, insert, related...)
}

{{end -}}

{{if $.AddPanic -}}
// Set{{$relAlias.Local}}P removes all previously related items of the
// {{$table.Name | singular}} replacing them completely with the passed
// in related items, optionally inserting them as new records.
// Sets o.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
// Replaces o.R.{{$relAlias.Local}} with related.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
{{- end}}
// Panics on error.
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}P({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, insert bool, related ...*{{$ftable.UpSingular}}) {
	if err := o.Set{{$relAlias.Local}}({{if not $.NoContext}}ctx, {{end -}} exec, insert, related...); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

{{if and $.AddGlobal $.AddPanic -}}
// Set{{$relAlias.Local}}GP removes all previously related items of the
// {{$table.Name | singular}} replacing them completely with the passed
// in related items, optionally inserting them as new records.
// Sets o.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
// Replaces o.R.{{$relAlias.Local}} with related.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
{{- end}}
// Uses the global database handle and panics on error.
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}GP({{if not $.NoContext}}ctx context.Context, {{end -}} insert bool, related ...*{{$ftable.UpSingular}}) {
	if err := o.Set{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, insert, related...); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

// Set{{$relAlias.Local}} removes all previously related items of the
// {{$table.Name | singular}} replacing them completely with the passed
// in related items, optionally inserting them as new records.
// Sets o.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
// Replaces o.R.{{$relAlias.Local}} with related.
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}'s {{$relAlias.Local}} accordingly.
{{- end}}
func (o *{{$ltable.UpSingular}}) Set{{$relAlias.Local}}({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, insert bool, related ...*{{$ftable.UpSingular}}) error {
	{{if not $.NoHooks -}}
	if err := o.doBeforeDeleteHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
		return errors.Wrap(err, "failed to perform before delete hooks")
	}
	{{- end}}
	
	{{if .ToJoinTable -}}
	query := "delete from {{.JoinTable | $.SchemaTable}} where {{.JoinLocalColumn | $.Quotes}} = {{if $.Dialect.UseIndexPlaceholders}}$1{{else}}?{{end}}"
	values := []interface{}{{"{"}}o.{{$col}}}
	{{else -}}
	query := "update {{.ForeignTable | $.SchemaTable}} set {{.ForeignColumn | $.Quotes}} = null where {{.ForeignColumn | $.Quotes}} = {{if $.Dialect.UseIndexPlaceholders}}$1{{else}}?{{end}}"
	values := []interface{}{{"{"}}o.{{$col}}}
	{{end -}}
	{{if $.NoContext -}}
	if boil.DebugMode {
		fmt.Fprintln(boil.DebugWriter, query)
		fmt.Fprintln(boil.DebugWriter, values)
	}
	{{else -}}
	if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
		fmt.Fprintln(writer, query)
		fmt.Fprintln(writer, values)
	}
	{{end -}}

	{{if $.NoContext -}}
	_, err := exec.Exec(query, values...)
	{{else -}}
	_, err := exec.ExecContext(ctx, query, values...)
	{{end -}}
	if err != nil {
		return errors.Wrap(err, "failed to remove relationships before set")
	}

	{{if not $.NoHooks -}}
	if err := o.doAfterDeleteHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
		return errors.Wrap(err, "failed to perform after delete hooks")
	}
	{{- end}}

	{{if and .ToJoinTable (not $.NoBackReferencing) -}}
	remove{{$relAlias.Local}}From{{$relAlias.Foreign}}Slice(o, related)
	if o.R != nil {
		o.R.{{$relAlias.Local}} = nil
	}
	{{else -}}
	if o.R != nil {
		{{if not $.NoBackReferencing -}}
		for _, rel := range o.R.{{$relAlias.Local}} {
			queries.SetScanner(&rel.{{$fcol}}, nil)
			if rel.R == nil {
				continue
			}

			rel.R.{{$relAlias.Foreign}} = nil
		}
		{{end -}}

		o.R.{{$relAlias.Local}} = nil
	}
	{{- end}}

	return o.Add{{$relAlias.Local}}({{if not $.NoContext}}ctx, {{end -}} exec, insert, related...)
}

{{if $.AddGlobal -}}
// Remove{{$relAlias.Local}}G relationships from objects passed in.
// Removes related items from R.{{$relAlias.Local}} (uses pointer comparison, removal does not keep order)
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}.
{{- end}}
// Uses the global database handle.
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}G({{if not $.NoContext}}ctx context.Context, {{end -}} related ...*{{$ftable.UpSingular}}) error {
	return o.Remove{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, related...)
}

{{end -}}

{{if $.AddPanic -}}
// Remove{{$relAlias.Local}}P relationships from objects passed in.
// Removes related items from R.{{$relAlias.Local}} (uses pointer comparison, removal does not keep order)
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}.
{{- end}}
// Panics on error.
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}P({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, related ...*{{$ftable.UpSingular}}) {
	if err := o.Remove{{$relAlias.Local}}({{if not $.NoContext}}ctx, {{end -}} exec, related...); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

{{if and $.AddGlobal $.AddPanic -}}
// Remove{{$relAlias.Local}}GP relationships from objects passed in.
// Removes related items from R.{{$relAlias.Local}} (uses pointer comparison, removal does not keep order)
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}.
{{- end}}
// Uses the global database handle and panics on error.
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}GP({{if not $.NoContext}}ctx context.Context, {{end -}} related ...*{{$ftable.UpSingular}}) {
	if err := o.Remove{{$relAlias.Local}}({{if $.NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, related...); err != nil {
		panic(boil.WrapErr(err))
	}
}

{{end -}}

// Remove{{$relAlias.Local}} relationships from objects passed in.
// Removes related items from R.{{$relAlias.Local}} (uses pointer comparison, removal does not keep order)
{{- if not $.NoBackReferencing}}
// Sets related.R.{{$relAlias.Foreign}}.
{{- end}}
func (o *{{$ltable.UpSingular}}) Remove{{$relAlias.Local}}({{if $.NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, related ...*{{$ftable.UpSingular}}) error {
	if len(related) == 0 {
		return nil
	}
	{{if not $.NoHooks -}}
	if err := o.doBeforeDeleteHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
		return errors.Wrap(err, "failed to perform before delete hooks")
	}
	{{- end}}
	var err error
	{{if .ToJoinTable -}}
	query := fmt.Sprintf(
		"delete from {{.JoinTable | $.SchemaTable}} where {{.JoinLocalColumn | $.Quotes}} = {{if $.Dialect.UseIndexPlaceholders}}$1{{else}}?{{end}} and {{.JoinForeignColumn | $.Quotes}} in (%s)",
		strmangle.Placeholders(dialect.UseIndexPlaceholders, len(related), 2, 1),
	)
	values := []interface{}{{"{"}}o.{{$col}}}
	for _, rel := range related {
		values = append(values, rel.{{$fcol}})
	}

	{{if $.NoContext -}}
	if boil.DebugMode {
		fmt.Fprintln(boil.DebugWriter, query)
		fmt.Fprintln(boil.DebugWriter, values)
	}
	{{else -}}
	if boil.IsDebug(ctx) {
		writer := boil.DebugWriterFrom(ctx)
		fmt.Fprintln(writer, query)
		fmt.Fprintln(writer, values)
	}
	{{end -}}

	{{if $.NoContext -}}
	_, err = exec.Exec(query, values...)
	{{else -}}
	_, err = exec.ExecContext(ctx, query, values...)
	{{end -}}
	if err != nil {
		return errors.Wrap(err, "failed to remove relationships before set")
	}
	{{else -}}
	for _, rel := range related {
		queries.SetScanner(&rel.{{$fcol}}, nil)
		{{if and (not .ToJoinTable) (not $.NoBackReferencing) -}}
		if rel.R != nil {
			rel.R.{{$relAlias.Foreign}} = nil
		}
		{{end -}}
		if {{if not $.NoRowsAffected}}_, {{end -}} err = rel.Update({{if not $.NoContext}}ctx, {{end -}} exec, boil.Whitelist("{{.ForeignColumn}}")); err != nil {
			return err
		}
	}
	{{end -}}

	{{if not $.NoHooks -}}
	if err := o.doAfterDeleteHooks({{if not $.NoContext}}ctx, {{end -}} exec); err != nil {
		return errors.Wrap(err, "failed to perform after delete hooks")
	}
	{{- end}}

	{{if and .ToJoinTable (not $.NoBackReferencing) -}}
	remove{{$relAlias.Local}}From{{$relAlias.Foreign}}Slice(o, related)
	{{end -}}
	if o.R == nil {
		return nil
	}

	for _, rel := range related {
		for i, ri := range o.R.{{$relAlias.Local}} {
			if rel != ri {
				continue
			}

			ln := len(o.R.{{$relAlias.Local}})
			if ln > 1 && i < ln-1 {
				o.R.{{$relAlias.Local}}[i] = o.R.{{$relAlias.Local}}[ln-1]
			}
			o.R.{{$relAlias.Local}} = o.R.{{$relAlias.Local}}[:ln-1]
			break
		}
	}

	return nil
}

{{if and .ToJoinTable (not $.NoBackReferencing) -}}
func remove{{$relAlias.Local}}From{{$relAlias.Foreign}}Slice(o *{{$ltable.UpSingular}}, related []*{{$ftable.UpSingular}}) {
	for _, rel := range related {
		if rel.R == nil {
			continue
		}
		for i, ri := range rel.R.{{$relAlias.Foreign}} {
			{{if $usesPrimitives -}}
			if o.{{$col}} != ri.{{$col}} {
			{{else -}}
			if !queries.Equal(o.{{$col}}, ri.{{$col}}) {
			{{end -}}
				continue
			}

			ln := len(rel.R.{{$relAlias.Foreign}})
			if ln > 1 && i < ln-1 {
				rel.R.{{$relAlias.Foreign}}[i] = rel.R.{{$relAlias.Foreign}}[ln-1]
			}
			rel.R.{{$relAlias.Foreign}} = rel.R.{{$relAlias.Foreign}}[:ln-1]
			break
		}
	}
}
				{{end -}}{{- /* if ToJoinTable */ -}}
			{{- end -}}{{- /* if nullable foreign key */ -}}
	{{- end -}}{{- /* range relationships */ -}}
{{- end -}}{{- /* if IsJoinTable */ -}}
