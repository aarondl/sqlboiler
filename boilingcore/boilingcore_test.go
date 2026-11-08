package boilingcore

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/aarondl/sqlboiler/v4/importers"
	"github.com/aarondl/strmangle"

	"github.com/aarondl/sqlboiler/v4/drivers"
	"github.com/aarondl/sqlboiler/v4/drivers/mocks"
)

var state *State
var rgxHasSpaces = regexp.MustCompile(`^\s+`)

func TestNew(t *testing.T) {
	testNew(t, Aliases{})
}

func TestNewWithCollidingTableNames(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	out, err := os.MkdirTemp("", "boil_templates_collision")
	if err != nil {
		t.Fatalf("unable to create tempdir: %s", err)
	}

	defer func() {
		if !t.Failed() {
			os.RemoveAll(out)
		}
	}()

	config := &Config{
		DriverName: "mock",
		PkgName:    "models",
		OutFolder:  out,
		NoTests:    true,
		DriverConfig: map[string]any{
			drivers.ConfigSchema:    "schema",
			drivers.ConfigWhitelist: []string{"personas"},
		},
		Imports:   importers.NewDefaultImports(),
		TagIgnore: []string{"pass"},
		Aliases:   Aliases{},
	}

	testState, err := New(config)
	if err != nil {
		t.Fatalf("Unable to create State using config: %s", err)
	}

	if err = testState.Run(); err != nil {
		t.Fatalf("Unable to execute State.Run: %s", err)
	}

	buf := &bytes.Buffer{}

	cmd := exec.Command("go", "env", "GOMOD")
	goModFilePath, err := cmd.Output()
	if err != nil {
		t.Fatalf("go env GOMOD cmd execution failed: %s", err)
	}

	cmd = exec.Command("go", "mod", "init", "github.com/aarondl/sqlboiler-test-collision")
	cmd.Dir = testState.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go mod init cmd execution failed: %s", err)
		outputCompileErrors(buf, testState.Config.OutFolder)
	}

	cmd = exec.Command("go", "mod", "edit", fmt.Sprintf("-replace=github.com/aarondl/sqlboiler/v4=%s", filepath.Dir(string(goModFilePath))))
	cmd.Dir = testState.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go mod edit replace cmd execution failed: %s", err)
		outputCompileErrors(buf, testState.Config.OutFolder)
	}

	cmd = exec.Command("go", "mod", "tidy")
	cmd.Dir = testState.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go mod tidy cmd execution failed: %s", err)
		outputCompileErrors(buf, testState.Config.OutFolder)
	}

	cmd = exec.Command("go", "build", ".")
	cmd.Dir = testState.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go build cmd execution failed: %s", err)
		outputCompileErrors(buf, testState.Config.OutFolder)
	}
}

func TestNewWithAliases(t *testing.T) {
	aliases := Aliases{Tables: make(map[string]TableAlias)}
	mockDriver := mocks.MockDriver{}
	tableNames, err := mockDriver.TableNames("", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i, tableName := range tableNames {
		tableAlias := TableAlias{
			UpPlural:     fmt.Sprintf("Alias%vThings", i),
			UpSingular:   fmt.Sprintf("Alias%vThing", i),
			DownPlural:   fmt.Sprintf("alias%vThings", i),
			DownSingular: fmt.Sprintf("alias%vThing", i),
		}
		columns, err := mockDriver.Columns("", tableName, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		tableAlias.Columns = make(map[string]string)
		for j, column := range columns {
			tableAlias.Columns[column.Name] = fmt.Sprintf("Alias%vThingColumn%v", i, j)
		}
		tableAlias.Relationships = make(map[string]RelationshipAlias)

		aliases.Tables[tableName] = tableAlias
	}

	testNew(t, aliases)
}

func testNew(t *testing.T, aliases Aliases) {
	if testing.Short() {
		t.SkipNow()
	}

	var err error
	out, err := os.MkdirTemp("", "boil_templates")
	if err != nil {
		t.Fatalf("unable to create tempdir: %s", err)
	}

	// Defer cleanup of the tmp folder
	defer func() {
		if t.Failed() {
			t.Log("template test output:", state.Config.OutFolder)
			return
		}
		os.RemoveAll(state.Config.OutFolder)
	}()

	config := &Config{
		DriverName: "mock",
		PkgName:    "models",
		OutFolder:  out,
		NoTests:    true,
		DriverConfig: map[string]any{
			drivers.ConfigSchema:    "schema",
			drivers.ConfigBlacklist: []string{"hangars"},
		},
		Imports:   importers.NewDefaultImports(),
		TagIgnore: []string{"pass"},
		Aliases:   aliases,
	}

	state, err = New(config)
	if err != nil {
		t.Fatalf("Unable to create State using config: %s", err)
	}

	if err = state.Run(); err != nil {
		t.Errorf("Unable to execute State.Run: %s", err)
	}

	buf := &bytes.Buffer{}

	cmd := exec.Command("go", "env", "GOMOD")
	goModFilePath, err := cmd.Output()
	if err != nil {
		t.Fatalf("go env GOMOD cmd execution failed: %s", err)
	}

	cmd = exec.Command("go", "mod", "init", "github.com/aarondl/sqlboiler-test")
	cmd.Dir = state.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go mod init cmd execution failed: %s", err)
		outputCompileErrors(buf, state.Config.OutFolder)
		fmt.Println()
	}

	cmd = exec.Command("go", "mod", "edit", fmt.Sprintf("-replace=github.com/aarondl/sqlboiler/v4=%s", filepath.Dir(string(goModFilePath))))
	cmd.Dir = state.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go mod init cmd execution failed: %s", err)
		outputCompileErrors(buf, state.Config.OutFolder)
		fmt.Println()
	}

	// From go1.16 dependencies are not auto downloaded
	cmd = exec.Command("go", "mod", "tidy")
	cmd.Dir = state.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go mod tidy cmd execution failed: %s", err)
		outputCompileErrors(buf, state.Config.OutFolder)
		fmt.Println()
	}

	cmd = exec.Command("go", "test", "-c")
	cmd.Dir = state.Config.OutFolder
	cmd.Stderr = buf

	if err = cmd.Run(); err != nil {
		t.Errorf("go test cmd execution failed: %s", err)
		outputCompileErrors(buf, state.Config.OutFolder)
		fmt.Println()
	}
}

func outputCompileErrors(buf *bytes.Buffer, outFolder string) {
	type errObj struct {
		errMsg     string
		fileName   string
		lineNumber int
	}

	var errObjects []errObj
	lineBuf := &bytes.Buffer{}

	bufLines := bytes.Split(buf.Bytes(), []byte{'\n'})
	for i := 0; i < len(bufLines); i++ {
		lineBuf.Reset()
		if !bytes.HasPrefix(bufLines[i], []byte("./")) {
			continue
		}

		fmt.Fprintf(lineBuf, "%s\n", bufLines[i])

		splits := bytes.Split(bufLines[i], []byte{':'})
		lineNum, err := strconv.Atoi(string(splits[1]))
		if err != nil {
			panic(fmt.Sprintf("Cant convert line number to int: %s", bufLines[i]))
		}

		eObj := errObj{
			fileName:   string(splits[0]),
			lineNumber: lineNum,
		}

		for y := i; y < len(bufLines); y++ {
			if !rgxHasSpaces.Match(bufLines[y]) {
				break
			}
			fmt.Fprintf(lineBuf, "%s\n", bufLines[y])
			i++
		}

		eObj.errMsg = lineBuf.String()

		errObjects = append(errObjects, eObj)
	}

	for _, eObj := range errObjects {
		fmt.Printf("-----------------\n")
		fmt.Println(eObj.errMsg)

		filePath := filepath.Join(outFolder, eObj.fileName)
		fh, err := os.Open(filePath)
		if err != nil {
			panic(fmt.Sprintf("Cant open the file: %#v", eObj))
		}

		scanner := bufio.NewScanner(fh)
		throwaway := eObj.lineNumber - 5
		for throwaway > 0 && scanner.Scan() {
			throwaway--
		}

		for i := 0; i < 6; i++ {
			if scanner.Scan() {
				b := scanner.Bytes()
				if len(b) != 0 {
					fmt.Printf("%s\n", b)
				} else {
					i--
				}
			}
		}

		fh.Close()
	}
}

func TestProcessTypeReplacements(t *testing.T) {
	s := new(State)
	s.Config = &Config{}
	s.Config.Imports.BasedOnType = make(map[string]importers.Set)
	s.Config.SkipReplacedEnumTypes = true
	domainStr := "a_domain"
	s.Tables = []drivers.Table{
		{
			Columns: []drivers.Column{
				{
					Name:     "id",
					Type:     "int",
					DBType:   "serial",
					Default:  "some db nonsense",
					Nullable: false,
				},
				{
					Name:     "name",
					Type:     "null.String",
					DBType:   "serial",
					Default:  "some db nonsense",
					Nullable: true,
				},
				{
					Name:       "domain",
					Type:       "int",
					DBType:     "numeric",
					Default:    "some db nonsense",
					DomainName: &domainStr,
					Nullable:   false,
				},
			},
		},
		{
			Name: "named_table",
			Columns: []drivers.Column{
				{
					Name:     "id",
					Type:     "int",
					DBType:   "serial",
					Default:  "some db nonsense",
					Nullable: false,
				},
			},
		},
	}

	s.Config.TypeReplaces = []TypeReplace{
		{
			Match: drivers.Column{
				DBType: "serial",
			},
			Replace: drivers.Column{
				Type: "excellent.Type",
			},
			Imports: importers.Set{
				ThirdParty: []string{`"rock.com/excellent"`},
			},
		},
		{
			Tables: []string{"named_table"},
			Match: drivers.Column{
				DBType: "serial",
			},
			Replace: drivers.Column{
				Type: "excellent.NamedType",
			},
			Imports: importers.Set{
				ThirdParty: []string{`"rock.com/excellent-name"`},
			},
		},
		{
			Match: drivers.Column{
				Type:     "null.String",
				Nullable: true,
			},
			Replace: drivers.Column{
				Type: "int",
			},
			Imports: importers.Set{
				Standard: []string{`"context"`},
			},
		},
		{
			Match: drivers.Column{
				DomainName: &domainStr,
			},
			Replace: drivers.Column{
				Type: "big.Int",
			},
			Imports: importers.Set{
				Standard: []string{`"math/big"`},
			},
		},
	}

	if err := s.processTypeReplacements(); err != nil {
		t.Fatal(err)
	}

	if typ := s.Tables[0].Columns[0].Type; typ != "excellent.Type" {
		t.Error("type was wrong:", typ)
	}
	if i := s.Config.Imports.BasedOnType["excellent.Type"].ThirdParty[0]; i != `"rock.com/excellent"` {
		t.Error("imports were not adjusted")
	}

	if typ := s.Tables[0].Columns[1].Type; typ != "int" {
		t.Error("type was wrong:", typ)
	}
	if i := s.Config.Imports.BasedOnType["int"].Standard[0]; i != `"context"` {
		t.Error("imports were not adjusted")
	}

	if typ := s.Tables[0].Columns[2].Type; typ != "big.Int" {
		t.Error("type was wrong:", typ)
	}
	if i := s.Config.Imports.BasedOnType["big.Int"].Standard[0]; i != `"math/big"` {
		t.Error("imports were not adjusted")
	}

	if typ := s.Tables[1].Columns[0].Type; typ != "excellent.NamedType" {
		t.Error("type was wrong:", typ)
	}
	if i := s.Config.Imports.BasedOnType["excellent.NamedType"].ThirdParty[0]; i != `"rock.com/excellent-name"` {
		t.Error("imports were not adjusted")
	}

	expectedDisallows := []string{
		"int", "null.String", // Must NOT contain replaced "excellent.Type".
	}
	if disallowList := s.Config.DiscardedEnumTypes; len(strmangle.SetComplement(disallowList, expectedDisallows)) != 0 {
		t.Error("expected disallows:", disallowList)
	}
}