# Changelog

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `CountDistinct` finisher to count the distinct values of a column (`COUNT(DISTINCT column)`).

## [4.19.7] - 2025-12-31

### Fixed

- Version number in main.go

## [4.19.6] - 2025-12-31

### Fixed

- Do not treat columns with partial unique indexes as unique in psql driver.

### Security

- Update golang.org/x/crypto from 0.38.0 -> 0.45.0

## [4.19.2-4] - 2025-06-26

### Changed

- Change import paths

## [4.19.1] - 2025-05-20

### Fixed

- Fix performance issue in `v4.19.0` by reverting the cleanup of unused imports.
- Updated minimum required Go version to 1.23.
- Updated dependencies to fix CVEs.

## [4.19.0] - 2025-05-09

### Added

- Add relation getters on base model structs. (thanks @parnic)
- Added `--no-relation-getters` option to prevent generation relation getters on the main structs. (thanks @parnic)
- Add Query, Exec, and Bind helpers to the global executor. (thanks @parnic)
  - BindGP
  - QueryRowG
  - QueryRowContextG
  - ExecG
  - ExecGP
  - QueryG
  - QueryGP
  - ExecContextG
  - ExecContextP
  - ExecContextGP
  - QueryContextG
  - QueryContextP
  - QueryContextGP

### Fixed

- Fix count from subquery on postgresql. (thanks @renom)
- Cleanup unused imports after code generation. (thanks @eklatzer)
- Fix InsertWhitelist test in composite primary key table. (thanks @benevolent0505)

## [4.18.0] - 2025-01-03

### Added

- Added `--no-schema` option scaffolding. (thanks @morganhein)

### Fixed

- Revert change to `DeleteAll` method. The change causes incorrect changes on tables with multi-column primary keys

## [4.17.1] - 2024-11-11

### Fixed

- Update the version constant to prevent inaccurate warnings about the version mismatch

## [4.17.0] - 2024-11-10

### Added

- Add MySQL unix socket support (thanks @c9s)
- Implement (Un-)marshalText for Decimal and NullDecimal (thanks @MJacred)
- Add version checking flags to make sure CLI and project runtime versions are the same (thanks @090809)
- Add SIMILAR TO method for Postgres (thanks @090809)
- Skip code generation for replaced enum types using the flag `--skip-replaced-enum-types` (thanks @MJacred)

### Fixed

- Fix compilation errors with TIMESTAMP columns in sqlite3 driver (thanks @hirasawayuki)
- Fix issue scanning `column_full_type` when `column_type` is NULL (thanks @mattdbush)
- Fix performance issue with `DeleteAll` by using a `WHERE IN` instead of `WHERE OR` (thanks @jakeiotechsys)
- Use renamed created column in `Update` method (thanks @glerchundi)
- Fix comment position in first column of table (thanks @hizzuu)
- Count from subquery if query uses HAVING or GROUP BY. This is because aggregate functions are run for each group separately, but we need to return the count of returned rows. (thanks @renom)
- Fix output filenames that contain a forward slash or backslash. Replace with an underscore (thanks @MJacred)

## [4.16.2] - 2024-02-12

### Fixed

- Replace `rand.seed` method to support golang 1.20 (thanks @pbr0ck3r)
- Fix issue with invalid template generation on ignored struct tags (thanks @090809)

## [4.16.1] - 2024-01-20

### Fixed

- Fix an issue caused in the last release where column names were double quoted (thanks @eirikbell)

## [4.16.0] - 2024-01-16

### Added

- Add `Ordinal` function to enum types (thanks @EmiPhil)
- Add ability to extend upsert expression with options (thanks @atzedus)
- Add support for different case style for different struct tags (thanks @c9s)

### Changed

- Improve loads by using maps for deduplication (thanks @nicowolf91)
- Return all columns not in both insert and update columns when doing upsert (thanks @adsa95)

### Fixed

- Composite foreign keys are now ignored to prevent generating invalid code (thanks @paulo-raca)
- Ignore cross-schema foreign keys to prevent invalid code gen (thanks @caleblloyd)
- Fix data race when registring hooks (thanks @nejtr0n)
- Fix types.JSON.MarshalJSON to handle nil values (thanks @agis)
- Properly quote column names in psql upsert (thanks @Flo4604)
- Use aliased field name for `LastInsertID` (thanks @motemen)
- Fix panic with nil pointers in structs to bind (thanks @stephenafamo)
- Use `sync.Map` for unique columns to prevent concurrent write and read (thanks @Maxibond)

## [4.15.0] - 2023-08-18

### Added

- Add LIKE and ILIKE Operators (thanks @jerrysalonen)
- Allow defining foreign key relationships in config file (thanks @chrisngyn)

### Fixed

- Fix missing types import on non nullable char column in postgres driver (thanks @Ebbele)
- Fix struct len check for columns whitelist and blacklist in sqlite driver (thanks @oscar-refacton)
- Properly clean name before title casing (thanks @MJacred)
- Fix parsing pgeo point in scientific notation format (thanks @zhongduo)
- Downgrade decimal version before decompose interface (thanks @zhongduo)
- Fix UPSERT query to respect conflict_target in postgres driver (thanks @agis)
- Fix column type conversion in SQLite3 driver, specifically, columns with types such as `FLOAT(2, 1)` or `TYNYINT(1)` (thanks @Jumpaku)

### Security

- When writing comments, also split comments by `\r` since several dialects also recognise this as a new line character

## [v4.14.2] - 2023-03-21

### Fixed

- Fix qm.WithDeleted helper with a custom soft delete column (thanks @lopezator)
- Skipping empty values from the update list (thanks @bvigar)

## [v4.14.1] - 2023-01-31

### Fixed

- Fix composite key handling in sqlite3 driver (thanks @vortura)
- Use correct executor for relationship test when `no-context` is true

## [v4.14.0] - 2022-12-14

### Added

- Allow calling struct.Exists() without having to pass on PK fields (thanks @MJacred)

### Changed

- Stop using deprecated methods from io/ioutil (thanks @stefafafan)

### Fixed

- Fixed correct hooks when loading relationships to-one (thanks @parnic)

## [v4.13.0] - 2022-08-28

### Added

- Generate IN/NIN whereHelpers for nullable types (thanks @fdegiuli)

### Fixed

- Fixed concurrent map writes in psql driver (thanks @pavel-krush)
- Force title case for enum null prefix (thanks @optiman)

## [v4.12.0] - 2022-07-26

### Added

- Fetch column and table info in parallel (thanks @pavel-krush)
- Generate IN/NIN methods for enum type query helpers (thanks @optiman)
- Improve psql performance by isolating uniqueness query (thanks @peterldowns)
- Support for psql materialized view (thanks @severedsea)
- Support loading model relationships when binding to a struct with embedded model (thanks @optiman)

### Fixed

- Fix panic when missing primary key in table (thanks @zapo)
- Fix some SQLite tests by enabling shared cache (thanks @gabe565)
- Fix(sqlite3): narrows `column.AutoGenerated` to columns with INT PKs (thanks @pbedat)
- Fix `auto-columns.deleted` not replace the default `deleted_at` column name (thanks @shivaluma)
- Trim whitespace for column comments (thanks @arp242)

## [v4.11.0] - 2022-04-25

### Added

- Add getter methods to relationship structs
  (thanks @fsaintjacques)

### Changed

- When title casing UPPER_SNAKE_CASE strings, underscores are not removed for readablity.

### Fixed

- Fix panic when a column referrring a foreign key is ignored
  (thanks @zapo)
- Fix one single point in paths and polygons
  (thanks @saulortega)

## [v4.10.2] - 2022-04-15

### Fixed

- Fix performance issue when scanning pgeo point (thanks @ivokanchev)

## [v4.10.1] - 2022-04-15

### Fixed

- Properly assign new query object in models.Pural()

## [v4.10.0] - 2022-04-15

### Added

- Add config options to allow user defined rules for inflections

### Fixed

- Don't generate test suites for views
- Properly assign new query object in models.Pural()
- Fix false negatives for enum values
- Strip non alphanumeric characters when title casing.

## [v4.9.2] - 2022-04-11

### Fixed

- Use correct column alias during soft delete
- Use a default "table.\*" for model queries

## [v4.9.1] - 2022-04-08

### Fixed

- Fixes issue with column name quotinc in many-to-many eager load
- Properly honor `--no-back-referencing` in relationship setops
- Retract `v4.9.0` due to issues with the commit tagging and the generated code showing `v4.8.6`

## [v4.9.0] - 2022-04-04

### Added

- Add `AllEnum()` function to retrieve a slice of all valid values of an enum type
- Add `DefaultTemplates` to `boilingcore.Config` to change the base template files to use for generation
- Add `CustomTemplateFuncs` to `boilingcore.Config` to supply additional functions that can be used in templates (thanks @ccakes)

### Fixed

- Fixes issues with detecting enum values that contain uppercases
- Properly wrap column names in quotes when loading many-to-many relationships (thanks @bryanmcgrane)
- Removes duplicated `deleted_at IS NULL` clause in relationship queries (thanks @ktakenaka)

## [v4.8.6] - 2022-01-29

### Added

- Add missing function `func (modelQuery) DeleteAllGP(...)` (thanks @parnic)

### Fixed

- Fixed issue with generation of both nullable and non-nullable enum types (thanks @optiman)

## [v4.8.5] - 2022-01-28

### Added

- Do not generate a template file if the content is empty
- Add function `drivers.RegisterBinaryFromCmdArg()` to extract binary registration

### Fixed

- Fix panic on zero value of `types.NullDecimal`
- `driver.Value()` for zero `types.Decimal` is now "0".

## [v4.8.4] - 2022-01-27

### Added

- Add new --always-wrap-errors flag that does not unwrap sql.ErrNoRows
  so it can retain the stack trace. This supports the best practice of using
  errors.Is() anyway and will eventually become the default behavior in
  a breaking v5 (thanks @jhnj)
- Add support for \* as a wildcard for white/blacklisting columns. See readme
  for details (thanks @Yoshiji)
- Add missing function `func (modelQuery) UpdateAllGP(...)` (thanks @MeanSquaredError)
- Add support for generated columns
- Add support for database views
- Add a `_model` suffix to the generated file for tables names that end with
  `_test` or `_goos` or `_goarch` since Go treats such files specially.
- Add `C` in front of model column attributes that begin with a number since a struct
  field cannot begin with a number in Go
- Add **sqlite3** driver to the main repo using the CGo-free port

### Changes

- Modify the `--add-enum-types` flag to also use the generated types in the model
  fields (thanks @optiman)
- Mark nullable columns as having a default in Postgres driver
- Bump MySQL version used for testing to 8.0

### Fixed

- Fix panic when a column referrring a foreign key is ignored
  (thanks @zapo)
- Fix bug with using the zero value of the decimal type for a nullable column
  (thanks @hongshaoyang)

## [v4.8.3] - 2021-11-16

### Fixed

- Fix bad use of titlecase in mysql enum name generation

## [v4.8.2] - 2021-11-16

### Fixed

- Fix regression in enum name generation

## [v4.8.1] - 2021-11-14

### Fixed

- Fix a regression in the soft delete test template generation introduced in
  4.8.1

## [v4.8.0] - 2021-11-14

### Added

- Add `--add-enum-types` to create distinct enum types instead of strings
  (thanks @stephenamo)

### Fixed

- Fix a regression in soft delete generation introduced in 4.7.1
  (thanks @stephenamo)

## [v4.7.1] - 2021-09-30

### Changed

- Change template locations to templates/{main,test}. This unfortunate move
  is necessary to preserve old behavior.

### Fixed

- Revert change to boilingcore.New() both in behavior and function signature

## [v4.7.0] - 2021-09-26

### Added

- Add configuration for overriding custom timestamp column names
  (thanks @stephanafamo)
- Add support for arguments to order by (thanks @emwalker and @alexdor)
- Add support for comments to mysql (thanks @Wuvist)

### Fixed

- Fix CVEs in transitive dependencies by bumping cobra & viper
- Fix inconsistent generation of IsNull/IsNotNull where helpers for types that
  appear both as null and not null in the database.
- JSON unmarshalling null into types.NullDecimal will no longer panic. String
  and format have been overridden to return "nil" when the underlying decimal
  is nil instead of crashing.

### Removed

- Removed bindata in favor of go:embed. This is not a breaking change as there
