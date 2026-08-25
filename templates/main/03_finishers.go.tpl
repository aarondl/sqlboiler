{{- $alias := .Aliases.Table .Table.Name}}

{{if .AddGlobal -}}
// OneG returns a single {{$alias.DownSingular}} record from the query using the global executor.
func (q {{$alias.DownSingular}}Query) OneG({{if not .NoContext}}ctx context.Context{{end}}) (*{{$alias.UpSingular}}, error) {
	return q.One({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
}

{{end -}}

{{if and .AddGlobal .AddPanic -}}
// OneGP returns a single {{$alias.DownSingular}} record from the query using the global executor, and panics on error.
func (q {{$alias.DownSingular}}Query) OneGP({{if not .NoContext}}ctx context.Context{{end}}) *{{$alias.UpSingular}} {
	o, err := q.One({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return o
}

{{end -}}

{{if .AddPanic -}}
// OneP returns a single {{$alias.DownSingular}} record from the query, and panics on error.
func (q {{$alias.DownSingular}}Query) OneP({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) (*{{$alias.UpSingular}}) {
	o, err := q.One({{if not .NoContext}}ctx, {{end -}} exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return o
}

{{end -}}

// One returns a single {{$alias.DownSingular}} record from the query.
func (q {{$alias.DownSingular}}Query) One({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) (*{{$alias.UpSingular}}, error) {
	o := &{{$alias.UpSingular}}{}

	queries.SetLimit(q.Query, 1)

	err := q.Bind({{if .NoContext}}nil{{else}}ctx{{end}}, exec, o)
	if err != nil {
		{{if not .AlwaysWrapErrors -}}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		{{end -}}
		return nil, errors.Wrap(err, "{{.PkgName}}: failed to execute a one query for {{.Table.Name}}")
	}

	{{if not .NoHooks -}}
	if err := o.doAfterSelectHooks({{if not .NoContext}}ctx, {{end -}} exec); err != nil {
		return o, err
	}
	{{- end}}

	return o, nil
}

{{if .AddGlobal -}}
// AllG returns all {{$alias.UpSingular}} records from the query using the global executor.
func (q {{$alias.DownSingular}}Query) AllG({{if not .NoContext}}ctx context.Context{{end}}) ({{$alias.UpSingular}}Slice, error) {
	return q.All({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
}

{{end -}}

{{if and .AddGlobal .AddPanic -}}
// AllGP returns all {{$alias.UpSingular}} records from the query using the global executor, and panics on error.
func (q {{$alias.DownSingular}}Query) AllGP({{if not .NoContext}}ctx context.Context{{end}}) {{$alias.UpSingular}}Slice {
	o, err := q.All({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return o
}

{{end -}}

{{if .AddPanic -}}
// AllP returns all {{$alias.UpSingular}} records from the query, and panics on error.
func (q {{$alias.DownSingular}}Query) AllP({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) {{$alias.UpSingular}}Slice {
	o, err := q.All({{if not .NoContext}}ctx, {{end -}} exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return o
}

{{end -}}

// All returns all {{$alias.UpSingular}} records from the query.
func (q {{$alias.DownSingular}}Query) All({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) ({{$alias.UpSingular}}Slice, error) {
	var o []*{{$alias.UpSingular}}

	err := q.Bind({{if .NoContext}}nil{{else}}ctx{{end}}, exec, &o)
	if err != nil {
		return nil, errors.Wrap(err, "{{.PkgName}}: failed to assign all query results to {{$alias.UpSingular}} slice")
	}

	{{if not .NoHooks -}}
	if len({{$alias.DownSingular}}AfterSelectHooks) != 0 {
		for _, obj := range o {
			if err := obj.doAfterSelectHooks({{if not .NoContext}}ctx, {{end -}} exec); err != nil {
				return o, err
			}
		}
	}
	{{- end}}

	return o, nil
}

{{if .AddGlobal -}}
// CountG returns the count of all {{$alias.UpSingular}} records in the query using the global executor
func (q {{$alias.DownSingular}}Query) CountG({{if not .NoContext}}ctx context.Context{{end}}) (int64, error) {
	return q.Count({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
}

{{end -}}

{{if and .AddGlobal .AddPanic -}}
// CountGP returns the count of all {{$alias.UpSingular}} records in the query using the global executor, and panics on error.
func (q {{$alias.DownSingular}}Query) CountGP({{if not .NoContext}}ctx context.Context{{end}}) int64 {
	c, err := q.Count({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return c
}

{{end -}}

{{if .AddPanic -}}
// CountP returns the count of all {{$alias.UpSingular}} records in the query, and panics on error.
func (q {{$alias.DownSingular}}Query) CountP({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) int64 {
	c, err := q.Count({{if not .NoContext}}ctx, {{end -}} exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return c
}

{{end -}}

// Count returns the count of all {{$alias.UpSingular}} records in the query.
func (q {{$alias.DownSingular}}Query) Count({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) (int64, error) {
	var count int64

	queries.SetSelect(q.Query, nil)
	queries.SetCount(q.Query)

	{{if .NoContext -}}
	err := q.Query.QueryRow(exec).Scan(&count)
	{{else -}}
	err := q.Query.QueryRowContext(ctx, exec).Scan(&count)
	{{end -}}
	if err != nil {
		return 0, errors.Wrap(err, "{{.PkgName}}: failed to count {{.Table.Name}} rows")
	}

	return count, nil
}

{{if .AddGlobal -}}
// CountDistinctG returns the count of distinct values of the given column in the query using the global executor.
func (q {{$alias.DownSingular}}Query) CountDistinctG({{if not .NoContext}}ctx context.Context, {{end}}column string) (int64, error) {
	return q.CountDistinct({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, column)
}

{{end -}}

{{if and .AddGlobal .AddPanic -}}
// CountDistinctGP returns the count of distinct values of the given column in the query using the global executor, and panics on error.
func (q {{$alias.DownSingular}}Query) CountDistinctGP({{if not .NoContext}}ctx context.Context, {{end}}column string) int64 {
	c, err := q.CountDistinct({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end}}, column)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return c
}

{{end -}}

{{if .AddPanic -}}
// CountDistinctP returns the count of distinct values of the given column in the query, and panics on error.
func (q {{$alias.DownSingular}}Query) CountDistinctP({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, column string) int64 {
	c, err := q.CountDistinct({{if not .NoContext}}ctx, {{end -}} exec, column)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return c
}

{{end -}}

// CountDistinct returns the count of distinct values of the given column in the query.
func (q {{$alias.DownSingular}}Query) CountDistinct({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}, column string) (int64, error) {
	var count int64

	queries.SetSelect(q.Query, nil)
	queries.SetCount(q.Query)
	queries.SetDistinct(q.Query, column)

	{{if .NoContext -}}
	err := q.Query.QueryRow(exec).Scan(&count)
	{{else -}}
	err := q.Query.QueryRowContext(ctx, exec).Scan(&count)
	{{end -}}
	if err != nil {
		return 0, errors.Wrap(err, "{{.PkgName}}: failed to count distinct {{.Table.Name}} rows")
	}

	return count, nil
}

{{if .AddGlobal -}}
// ExistsG checks if the row exists in the table using the global executor.
func (q {{$alias.DownSingular}}Query) ExistsG({{if not .NoContext}}ctx context.Context{{end}}) (bool, error) {
	return q.Exists({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
}

{{end -}}

{{if and .AddGlobal .AddPanic -}}
// ExistsGP checks if the row exists in the table using the global executor, and panics on error.
func (q {{$alias.DownSingular}}Query) ExistsGP({{if not .NoContext}}ctx context.Context{{end}}) bool {
	e, err := q.Exists({{if .NoContext}}boil.GetDB(){{else}}ctx, boil.GetContextDB(){{end -}})
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return e
}

{{end -}}

{{if .AddPanic -}}
// ExistsP checks if the row exists in the table, and panics on error.
func (q {{$alias.DownSingular}}Query) ExistsP({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) bool {
	e, err := q.Exists({{if not .NoContext}}ctx, {{end -}} exec)
	if err != nil {
		panic(boil.WrapErr(err))
	}

	return e
}

{{end -}}

// Exists checks if the row exists in the table.
func (q {{$alias.DownSingular}}Query) Exists({{if .NoContext}}exec boil.Executor{{else}}ctx context.Context, exec boil.ContextExecutor{{end}}) (bool, error) {
	var count int64

	queries.SetSelect(q.Query, nil)
	queries.SetCount(q.Query)
	queries.SetLimit(q.Query, 1)

	{{if .NoContext -}}
	err := q.Query.QueryRow(exec).Scan(&count)
	{{else -}}
	err := q.Query.QueryRowContext(ctx, exec).Scan(&count)
	{{end -}}
	if err != nil {
		return false, errors.Wrap(err, "{{.PkgName}}: failed to check if {{.Table.Name}} exists")
	}

	return count > 0, nil
}
