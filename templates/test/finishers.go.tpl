{{- $alias := .Aliases.Table .Table.Name}}
func test{{$alias.UpPlural}}Bind(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &{{$alias.UpSingular}}{}
	if err = randomize.Struct(seed, o, {{$alias.DownSingular}}DBTypes, true, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}

	{{if not .NoContext}}ctx := context.Background(){{end}}
	tx := MustTx({{if .NoContext}}boil.Begin(){{else}}boil.BeginTx(ctx, nil){{end}})
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if err = {{$alias.UpPlural}}().Bind({{if .NoContext}}nil{{else}}ctx{{end}}, tx, o); err != nil {
		t.Error(err)
	}
}

func test{{$alias.UpPlural}}One(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	o := &{{$alias.UpSingular}}{}
	if err = randomize.Struct(seed, o, {{$alias.DownSingular}}DBTypes, true, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}

	{{if not .NoContext}}ctx := context.Background(){{end}}
	tx := MustTx({{if .NoContext}}boil.Begin(){{else}}boil.BeginTx(ctx, nil){{end}})
	defer func() { _ = tx.Rollback() }()
	if err = o.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	if x, err := {{$alias.UpPlural}}().One({{if not .NoContext}}ctx, {{end -}} tx); err != nil {
		t.Error(err)
	} else if x == nil {
		t.Error("expected to get a non nil record")
	}
}

func test{{$alias.UpPlural}}All(t *testing.T) {
	t.Parallel()

	seed := randomize.NewSeed()
	var err error
	{{$alias.DownSingular}}One := &{{$alias.UpSingular}}{}
	{{$alias.DownSingular}}Two := &{{$alias.UpSingular}}{}
	if err = randomize.Struct(seed, {{$alias.DownSingular}}One, {{$alias.DownSingular}}DBTypes, false, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}
	if err = randomize.Struct(seed, {{$alias.DownSingular}}Two, {{$alias.DownSingular}}DBTypes, false, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}

	{{if not .NoContext}}ctx := context.Background(){{end}}
	tx := MustTx({{if .NoContext}}boil.Begin(){{else}}boil.BeginTx(ctx, nil){{end}})
	defer func() { _ = tx.Rollback() }()
	if err = {{$alias.DownSingular}}One.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = {{$alias.DownSingular}}Two.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	slice, err := {{$alias.UpPlural}}().All({{if not .NoContext}}ctx, {{end -}} tx)
	if err != nil {
		t.Error(err)
	}

	if len(slice) != 2 {
		t.Error("want 2 records, got:", len(slice))
	}
}

func test{{$alias.UpPlural}}Count(t *testing.T) {
	t.Parallel()

	var err error
	seed := randomize.NewSeed()
	{{$alias.DownSingular}}One := &{{$alias.UpSingular}}{}
	{{$alias.DownSingular}}Two := &{{$alias.UpSingular}}{}
	if err = randomize.Struct(seed, {{$alias.DownSingular}}One, {{$alias.DownSingular}}DBTypes, false, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}
	if err = randomize.Struct(seed, {{$alias.DownSingular}}Two, {{$alias.DownSingular}}DBTypes, false, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}

	{{if not .NoContext}}ctx := context.Background(){{end}}
	tx := MustTx({{if .NoContext}}boil.Begin(){{else}}boil.BeginTx(ctx, nil){{end}})
	defer func() { _ = tx.Rollback() }()
	if err = {{$alias.DownSingular}}One.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = {{$alias.DownSingular}}Two.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	count, err := {{$alias.UpPlural}}().Count({{if not .NoContext}}ctx, {{end -}} tx)
	if err != nil {
		t.Error(err)
	}

	if count != 2 {
		t.Error("want 2 records, got:", count)
	}
}

func test{{$alias.UpPlural}}CountDistinct(t *testing.T) {
	t.Parallel()

	var err error
	seed := randomize.NewSeed()
	{{$alias.DownSingular}}One := &{{$alias.UpSingular}}{}
	{{$alias.DownSingular}}Two := &{{$alias.UpSingular}}{}
	if err = randomize.Struct(seed, {{$alias.DownSingular}}One, {{$alias.DownSingular}}DBTypes, false, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}
	if err = randomize.Struct(seed, {{$alias.DownSingular}}Two, {{$alias.DownSingular}}DBTypes, false, {{$alias.DownSingular}}ColumnsWithDefault...); err != nil {
		t.Errorf("Unable to randomize {{$alias.UpSingular}} struct: %s", err)
	}

	{{if not .NoContext}}ctx := context.Background(){{end}}
	tx := MustTx({{if .NoContext}}boil.Begin(){{else}}boil.BeginTx(ctx, nil){{end}})
	defer func() { _ = tx.Rollback() }()
	if err = {{$alias.DownSingular}}One.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}
	if err = {{$alias.DownSingular}}Two.Insert({{if not .NoContext}}ctx, {{end -}} tx, boil.Infer()); err != nil {
		t.Error(err)
	}

	// Two rows were inserted, so a distinct count of any single column is between 1 and 2.
	count, err := {{$alias.UpPlural}}().CountDistinct({{if not .NoContext}}ctx, {{end -}} tx, "{{ (index $.Table.Columns 0).Name | $.Quotes }}")
	if err != nil {
		t.Error(err)
	}

	if count < 1 || count > 2 {
		t.Error("want a distinct count between 1 and 2, got:", count)
	}
}
