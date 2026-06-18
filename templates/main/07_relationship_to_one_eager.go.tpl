{{- if or .Table.IsJoinTable .Table.IsView -}}
{{- else -}}
	{{- range $fkey := .Table.FKeys -}}
		{{- $ltable := $.Aliases.Table $fkey.Table -}}
		{{- $ftable := $.Aliases.Table $fkey.ForeignTable -}}
		{{- $rel := $ltable.Relationship $fkey.Name -}}
		{{- $arg := printf "maybe%s" $ltable.UpSingular -}}
		{{- $col := $ltable.Column $fkey.Column -}}
		{{- $fcol := $ftable.Column $fkey.ForeignColumn -}}
		{{- $usesPrimitives := usesPrimitives $.Tables $fkey.Table $fkey.Column $fkey.ForeignTable $fkey.ForeignColumn -}}
		{{- $canSoftDelete := (getTable $.Tables $fkey.ForeignTable).CanSoftDelete $.AutoColumns.Deleted }}
// Load{{$rel.Foreign}} allows an eager lookup of values, cached into the
// loaded structs of the objects. This is for an N-1 relationship.
func ({{$ltable.DownSingular}}L) Load{{$rel.Foreign}}({{if $.NoContext}}e boil.Executor{{else}}ctx context.Context, e boil.ContextExecutor{{end}}, singular bool, {{$arg}} any, mods queries.Applicator) error {
	var slice []*{{$ltable.UpSingular}}
	var object *{{$ltable.UpSingular}}

	if singular {
		var ok bool
		object, ok = {{$arg}}.(*{{$ltable.UpSingular}})
		if !ok {
			object = new({{$ltable.UpSingular}})
			ok = queries.SetFromEmbeddedStruct(&object, &{{$arg}})
			if !ok {
				return errors.New(fmt.Sprintf("failed to set %T from embedded struct %T", object, {{$arg}}))
			}
		}
	} else {
		s, ok := {{$arg}}.(*[]*{{$ltable.UpSingular}})
		if ok {
			slice = *s
		} else {
			ok = queries.SetFromEmbeddedStruct(&slice, {{$arg}})
			if !ok {
				return errors.New(fmt.Sprintf("failed to set %T from embedded struct %T", slice, {{$arg}}))
			}
		}
	}

	args := make(map[any]struct{})
	if singular {
		if object.R == nil {
			object.R = &{{$ltable.DownSingular}}R{}
		}
		{{if $usesPrimitives -}}
		args[object.{{$col}}] = struct{}{}
		{{else -}}
		if !queries.IsNil(object.{{$col}}) {
			args[object.{{$col}}] = struct{}{}
		}
		{{end}}
	} else {
		for _, obj := range slice {
			if obj.R == nil {
				obj.R = &{{$ltable.DownSingular}}R{}
			}

			{{if $usesPrimitives -}}
			args[obj.{{$col}}] = struct{}{}
			{{else -}}
			if !queries.IsNil(obj.{{$col}}) {
				args[obj.{{$col}}] = struct{}{}
			}
			{{end}}
		}
	}

	if len(args) == 0 {
		return nil
	}

	argsSlice := make([]any, len(args))
	i := 0
	for arg := range args {
		argsSlice[i] = arg
		i++
	}

	query := NewQuery(
		qm.From(`{{if $.Dialect.UseSchema}}{{$.Schema}}.{{end}}{{.ForeignTable}}`),
		qm.WhereIn(`{{if $.Dialect.UseSchema}}{{$.Schema}}.{{end}}{{.ForeignTable}}.{{.ForeignColumn}} in ?`, argsSlice...),
		{{if and $.AddSoftDeletes $canSoftDelete -}}
		qmhelper.WhereIsNull(`{{if $.Dialect.UseSchema}}{{$.Schema}}.{{end}}{{.ForeignTable}}.{{or $.AutoColumns.Deleted "deleted_at"}}`),
		{{- end}}
	)
	if mods != nil {
		mods.Apply(query)
	}

	{{if $.NoContext -}}
	results, err := query.Query(e)
	{{else -}}
	results, err := query.QueryContext(ctx, e)
	{{end -}}
	if err != nil {
		return errors.Wrap(err, "failed to eager load {{$ftable.UpSingular}}")
	}

	var resultSlice []*{{$ftable.UpSingular}}
	if err = queries.Bind(results, &resultSlice); err != nil {
		return errors.Wrap(err, "failed to bind eager loaded slice {{$ftable.UpSingular}}")
	}

	if err = results.Close(); err != nil {
		return errors.Wrap(err, "failed to close results of eager load for {{.ForeignTable}}")
	}
	if err = results.Err(); err != nil {
		return errors.Wrap(err, "error occurred during iteration of eager loaded relations for {{.ForeignTable}}")
	}

	{{if not $.NoHooks -}}
	if len({{$ftable.DownSingular}}AfterSelectHooks) != 0 {
		for _, obj := range resultSlice {
			if err := obj.doAfterSelectHooks({{if $.NoContext}}e{{else}}ctx, e{{end}}); err != nil {
				return err
			}
		}
	}
	{{- end}}

	if len(resultSlice) == 0 {
		return nil
	}

	if singular {
		foreign := resultSlice[0]
		object.R.{{$rel.Foreign}} = foreign
		{{if not $.NoBackReferencing -}}
		if foreign.R == nil {
			foreign.R = &{{$ftable.DownSingular}}R{}
		}
			{{if $fkey.Unique -}}
		foreign.R.{{$rel.Local}} = object
			{{else -}}
		foreign.R.{{$rel.Local}} = append(foreign.R.{{$rel.Local}}, object)
			{{end -}}
		{{end -}}
		return nil
	}

	resultMap := make(map[string]*{{$ftable.UpSingular}})
	for _, r := range resultSlice {
		resultMap[generateMapKey(r.{{$fcol}})] = r
	}

	for _, local := range slice {
		if foreign, ok := resultMap[generateMapKey(local.{{$col}})]; ok {
			local.R.{{$rel.Foreign}} = foreign
			{{if not $.NoBackReferencing -}}
			if foreign.R == nil {
				foreign.R = &{{$ftable.DownSingular}}R{}
			}
			{{if $fkey.Unique -}}
			foreign.R.{{$rel.Local}} = local
			{{else -}}
			foreign.R.{{$rel.Local}} = append(foreign.R.{{$rel.Local}}, local)
			{{end -}}
			{{end -}}
		}
	}

	return nil
}
{{end -}}{{/* range */}}
{{end}}{{/* join table */}}
