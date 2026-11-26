// M type is for providing columns and column values to UpdateAll.
type M map[string]any

// ErrSyncFail occurs during insert when the record could not be retrieved in
// order to populate default value information. This usually happens when LastInsertId
// fails or there was a primary key configuration that was not resolvable.
var ErrSyncFail = errors.New("{{.PkgName}}: failed to synchronize data after insert")

type insertCache struct {
	query        string
	retQuery     string
	valueMapping []uint64
	retMapping   []uint64
}

type updateCache struct {
	query        string
	valueMapping []uint64
}

func makeCacheKey(cols boil.Columns, nzDefaults []string) string {
	buf := strmangle.GetBuffer()

	buf.WriteString(strconv.Itoa(cols.Kind))
	for _, w := range cols.Cols {
		buf.WriteString(w)
	}

	if len(nzDefaults) != 0 {
		buf.WriteByte('.')
	}
	for _, nz := range nzDefaults {
		buf.WriteString(nz)
	}

	str := buf.String()
	strmangle.PutBuffer(buf)
	return str
}

// generateMapKey converts a database key value to its string representation.
func generateMapKey(value any) string {
	switch v := value.(type) {
	// string
	case string:
		return v

	// byte slice
	case []byte:
		return string(v)

	// integers
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)

	// float: uncommon as DB keys but supported for completeness
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)

	// sql driver.Valuer: unwrap and recurse on the underlying value
	case driver.Valuer:
		valuerValue, err := v.Value()
		if err != nil || valuerValue == nil {
			return ""
		}
		return generateMapKey(valuerValue)

	// fallback for other types
	default:
		return fmt.Sprintf("%v", value)
	}
}

{{/*
The following is a little bit of black magic and deserves some explanation

Because postgres and mysql define enums completely differently (one at the
database level as a custom datatype, and one at the table column level as
a unique thing per table)... There's a chance the enum is named (postgres)
and not (mysql). So we can't do this per table so this code is here.

We loop through each table and column looking for enums. If it's named, we
then use some disgusting magic to write state during the template compile to
the "once" map. This lets named enums only be defined once if they're referenced
multiple times in many (or even the same) tables.

Then we check if all it's values are normal, if they are we create the enum
output, if not we output a friendly error message as a comment to aid in
debugging.

Postgres output looks like: EnumNameEnumValue = "enumvalue"
MySQL output looks like:    TableNameColNameEnumValue = "enumvalue"

It only titlecases the EnumValue portion if it's snake-cased.
*/}}
{{$once := onceNew}}
{{$onceNull := onceNew}}
{{$onceNullEnum := onceNew}}
{{$ignoredEnumTypes := .DiscardedEnumTypes -}}
{{/* With enum-style go-enums, enum types embed enum.StringEnum from github.com/paulo-raca/go-enums instead of being a string type. */ -}}
{{$goEnums := and .AddEnumTypes (eq .EnumStyle "go-enums") -}}
{{- range $table := .Tables -}}
	{{- range $col := $table.Columns | filterColumnsByEnum -}}
		{{- $name := parseEnumName $col.DBType -}}
		{{- $vals := parseEnumVals $col.DBType -}}
		{{- $isNamed := ne (len $name) 0}}
		{{- $enumName := "" -}}
		{{- if not (and
			$isNamed
			(and
				($once.Has $name)
				($onceNull.Has $name)
			)
		) -}}
			{{- if gt (len $vals) 0}}
				{{- if $isNamed -}}
					{{ $enumName = titleCase $name}}
				{{- else -}}
					{{ $enumName = printf "%s%s" (titleCase $table.Name) (titleCase $col.Name)}}
				{{- end -}}
				{{if containsAny $ignoredEnumTypes $enumName -}}
					{{continue}}
				{{end -}}
				{{/* First iteration for enum type $name (nullable or not) */}}
				{{- $enumFirstIter := and
					(not ($once.Has $name))
					(not ($onceNull.Has $name))
				-}}

				{{- if and $enumFirstIter $goEnums -}}
					type {{$enumName}} struct {
						enum.StringEnum[{{$enumName}}]
					}

					// Enum values for {{$enumName}}
					var (
					{{range $val := $vals -}}
						{{- $enumValue := titleCase $val -}}
						{{$enumName}}{{$enumValue}} = enum.New[{{$enumName}}]({{printf "%q" $val}})
					{{end -}}
					)
				{{- else if $enumFirstIter -}}
					{{$enumType := "string" }}
					{{$allvals := "\n"}}

					{{if $.AddEnumTypes}}
						{{- $enumType = $enumName -}}
						type {{$enumName}} string
					{{end}}

					// Enum values for {{$enumName}}
					const (
					{{range $val := $vals -}}
						{{- $enumValue := titleCase $val -}}
						{{$enumName}}{{$enumValue}} {{$enumType}} = {{printf "%q" $val}}
						{{$allvals = printf "%s%s%s,\n" $allvals $enumName $enumValue -}}
					{{end -}}
					)

					func All{{$enumName}}() []{{$enumType}} {
						return []{{$enumType}}{ {{$allvals}} }
					}
				{{- end -}}

				{{if $.AddEnumTypes}}
					{{ if and $enumFirstIter $goEnums }}
						// Randomize implements randomize.Randomizer so that the generated tests
						// pick one of the declared values.
						func (e *{{$enumName}}) Randomize(nextInt func() int64, fieldType string, shouldBeNull bool) {
							values := enum.Values[{{$enumName}}]()
							*e = values[uint64(nextInt())%uint64(len(values))]
						}
					{{- end}}

					{{ if and $enumFirstIter (not $goEnums) }}
						func (e {{$enumName}}) IsValid() error {
							{{- /* $first is being used to add a comma to all enumValues, but the first one.*/ -}}
							{{- $first := true -}}
							{{- /* $enumValues will contain a comma separated string holding all enum consts */ -}}
							{{- $enumValues := "" -}}
							{{ range $val := $vals -}}
								{{- if $first -}}
									{{- $first = false -}}
								{{- else -}}
									{{- $enumValues = printf "%s%s" $enumValues ", " -}}
								{{- end -}}

								{{- $enumValue := titleCase $val -}}
								{{- $enumValues = printf "%s%s%s" $enumValues $enumName $enumValue -}}
							{{- end}}
							switch e {
							case {{$enumValues}}:
								return nil
							default:
								return errors.New("enum is not valid")
							}
						}

						func (e {{$enumName}}) String() string {
							return string(e)
						}

						func (e {{$enumName}}) Ordinal() int {
							switch e {
							{{range $idx, $val := $vals -}}
								{{- $enumValue := titleCase $val -}}
								case {{$enumName}}{{$enumValue}}:
									return {{$idx}}
							{{end}}
							default:
								panic(errors.New("enum is not valid"))
							}
						}
					{{- end -}}

					{{ if and
						$col.Nullable
						(not ($onceNull.Has $name))
					}}
						{{$enumType := ""}}
						{{- if $isNamed -}}
							{{- $enumType = (print (titleCase $.EnumNullPrefix) $enumName) }}
						{{- else -}}
							{{- $enumType = printf "%s%s" (titleCase $table.Name) (print (titleCase $.EnumNullPrefix) (titleCase $col.Name)) -}}
						{{- end -}}
						{{- if $goEnums -}}
							{{- $generic := printf "%sEnum" (titleCase $.EnumNullPrefix) -}}
							{{- if not ($onceNullEnum.Has $generic) -}}
								{{- $_ := $onceNullEnum.Put $generic -}}
								// {{$generic}} is a nullable go-enums enum. It supports SQL and JSON serialization,
								// delegating the conversion of the non-null value to the enum itself.
								type {{$generic}}[T enum.Enum[T]] struct {
									Val   T
									Valid bool
								}

								// New{{$generic}} creates a new {{$generic}}.
								func New{{$generic}}[T enum.Enum[T]](v T, valid bool) {{$generic}}[T] {
									return {{$generic}}[T]{Val: v, Valid: valid}
								}

								// {{$generic}}From creates a new {{$generic}} that will never be null.
								func {{$generic}}From[T enum.Enum[T]](v T) {{$generic}}[T] {
									return New{{$generic}}(v, true)
								}

								// {{$generic}}FromPtr creates a new {{$generic}} that is null if v is nil.
								func {{$generic}}FromPtr[T enum.Enum[T]](v *T) {{$generic}}[T] {
									if v == nil {
										return {{$generic}}[T]{}
									}
									return New{{$generic}}(*v, true)
								}

								// UnmarshalJSON implements json.Unmarshaler.
								func (e *{{$generic}}[T]) UnmarshalJSON(data []byte) error {
									if bytes.Equal(data, null.NullBytes) {
										*e = {{$generic}}[T]{}
										return nil
									}
									if err := json.Unmarshal(data, &e.Val); err != nil {
										return err
									}
									e.Valid = true
									return nil
								}

								// MarshalJSON implements json.Marshaler.
								func (e {{$generic}}[T]) MarshalJSON() ([]byte, error) {
									if !e.Valid {
										return null.NullBytes, nil
									}
									return json.Marshal(e.Val)
								}

								// MarshalText implements encoding.TextMarshaler.
								func (e {{$generic}}[T]) MarshalText() ([]byte, error) {
									if !e.Valid {
										return []byte{}, nil
									}
									return any(e.Val).(interface{ MarshalText() ([]byte, error) }).MarshalText()
								}

								// UnmarshalText implements encoding.TextUnmarshaler.
								func (e *{{$generic}}[T]) UnmarshalText(text []byte) error {
									if len(text) == 0 {
										*e = {{$generic}}[T]{}
										return nil
									}
									if err := any(&e.Val).(interface{ UnmarshalText([]byte) error }).UnmarshalText(text); err != nil {
										return err
									}
									e.Valid = true
									return nil
								}

								// SetValid changes this {{$generic}} value and also sets it to be non-null.
								func (e *{{$generic}}[T]) SetValid(v T) {
									e.Val = v
									e.Valid = true
								}

								// Ptr returns a pointer to this {{$generic}} value, or a nil pointer if it is null.
								func (e {{$generic}}[T]) Ptr() *T {
									if !e.Valid {
										return nil
									}
									return &e.Val
								}

								// IsZero returns true for null types.
								func (e {{$generic}}[T]) IsZero() bool {
									return !e.Valid
								}

								// Scan implements the Scanner interface.
								func (e *{{$generic}}[T]) Scan(value any) error {
									if value == nil {
										*e = {{$generic}}[T]{}
										return nil
									}
									if err := any(&e.Val).(interface{ Scan(any) error }).Scan(value); err != nil {
										return err
									}
									e.Valid = true
									return nil
								}

								// Value implements the driver Valuer interface.
								func (e {{$generic}}[T]) Value() (driver.Value, error) {
									if !e.Valid {
										return nil, nil
									}
									return any(e.Val).(driver.Valuer).Value()
								}

								// Randomize implements randomize.Randomizer so that the generated tests
								// pick one of the declared values, or null.
								func (e *{{$generic}}[T]) Randomize(nextInt func() int64, fieldType string, shouldBeNull bool) {
									if shouldBeNull {
										*e = {{$generic}}[T]{}
										return
									}
									values := enum.Values[T]()
									e.Val = values[uint64(nextInt())%uint64(len(values))]
									e.Valid = true
								}
							{{end}}

							// {{$enumType}} is a nullable {{$enumName}} enum type. It supports SQL and JSON serialization.
							type {{$enumType}} = {{$generic}}[{{$enumName}}]
						{{- else -}}
						// {{$enumType}} is a nullable {{$enumName}} enum type. It supports SQL and JSON serialization.
						type {{$enumType}} struct {
							Val		{{$enumName}}
							Valid	bool
						}

						// {{$enumType}}From creates a new {{$enumName}} that will never be blank.
						func {{$enumType}}From(v {{$enumName}}) {{$enumType}} {
							return New{{$enumType}}(v, true)
						}

						// {{$enumType}}FromPtr creates a new {{$enumType}} that be null if s is nil.
						func {{$enumType}}FromPtr(v *{{$enumName}}) {{$enumType}} {
							if v == nil {
								return New{{$enumType}}("", false)
							}
							return New{{$enumType}}(*v, true)
						}

						// New{{$enumType}} creates a new {{$enumType}}
						func New{{$enumType}}(v {{$enumName}}, valid bool) {{$enumType}} {
							return {{$enumType}}{
								Val:	v,
								Valid:  valid,
							}
						}

						// UnmarshalJSON implements json.Unmarshaler.
						func (e *{{$enumType}}) UnmarshalJSON(data []byte) error {
							if bytes.Equal(data, null.NullBytes) {
								e.Val = ""
								e.Valid = false
								return nil
							}

							if err := json.Unmarshal(data, &e.Val); err != nil {
								return err
							}

							e.Valid = true
							return nil
						}

						// MarshalJSON implements json.Marshaler.
						func (e {{$enumType}}) MarshalJSON() ([]byte, error) {
							if !e.Valid {
								return null.NullBytes, nil
							}
							return json.Marshal(e.Val)
						}

						// MarshalText implements encoding.TextMarshaler.
						func (e {{$enumType}}) MarshalText() ([]byte, error) {
							if !e.Valid {
								return []byte{}, nil
							}
							return []byte(e.Val), nil
						}

						// UnmarshalText implements encoding.TextUnmarshaler.
						func (e *{{$enumType}}) UnmarshalText(text []byte) error {
							if text == nil || len(text) == 0 {
								e.Valid = false
								return nil
							}

							e.Val = {{$enumName}}(text)
							e.Valid = true
							return nil
						}

						// SetValid changes this {{$enumType}} value and also sets it to be non-null.
						func (e *{{$enumType}}) SetValid(v {{$enumName}}) {
							e.Val = v
							e.Valid = true
						}

						// Ptr returns a pointer to this {{$enumType}} value, or a nil pointer if this {{$enumType}} is null.
						func (e {{$enumType}}) Ptr() *{{$enumName}} {
							if !e.Valid {
								return nil
							}
							return &e.Val
						}

						// IsZero returns true for null types.
						func (e {{$enumType}}) IsZero() bool {
							return !e.Valid
						}

						// Scan implements the Scanner interface.
						func (e *{{$enumType}}) Scan(value any) error {
							if value == nil {
								e.Val, e.Valid = "", false
								return nil
							}
							e.Valid = true
							return convert.ConvertAssign((*string)(&e.Val), value)
						}

						// Value implements the driver Valuer interface.
						func (e {{$enumType}}) Value() (driver.Value, error) {
							if !e.Valid {
								return nil, nil
							}
							return string(e.Val), nil
						}
						{{- end}}
					{{end -}}
				{{end -}}
			{{else}}
				// Enum values for {{$table.Name}} {{$col.Name}} are not proper Go identifiers, cannot emit constants
			{{- end -}}
			{{/* Save column type name after generation.
			 Needs to be at the bottom because we check for the first iteration
			 inside the $table.Columns loop. */}}
			{{- if $isNamed -}}
				{{- if $col.Nullable -}}
					{{$_ := $onceNull.Put $name}}
				{{- else -}}
					{{$_ := $once.Put $name}}
				{{- end -}}
			{{- end -}}
		{{- end -}}
	{{- end -}}
{{ end -}}