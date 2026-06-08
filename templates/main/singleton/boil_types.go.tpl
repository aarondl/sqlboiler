// Valuer is a type alias for database/sql/driver used for type assertion (e.g. in eager loading optimization).
type Valuer = driver.Valuer

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
{{$ignoredEnumTypes := .DiscardedEnumTypes -}}
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

				{{- if $enumFirstIter -}}
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
					{{ if $enumFirstIter }}
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