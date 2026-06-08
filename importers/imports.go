// Package importers helps with dynamic imports for templating
package importers

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cast"

	"github.com/aarondl/strmangle"
	"github.com/friendsofgo/errors"
)

// Collection of imports for various templating purposes
// Drivers add to any and all of these, and is completely responsible
// for populating BasedOnType.
type Collection struct {
	All  Set `toml:"all" json:"all,omitempty"`
	Test Set `toml:"test" json:"test,omitempty"`

	Singleton     Map `toml:"singleton" json:"singleton,omitempty"`
	TestSingleton Map `toml:"test_singleton" json:"test_singleton,omitempty"`

	BasedOnType Map `toml:"based_on_type" json:"based_on_type,omitempty"`
}

// Set defines the optional standard imports and
// thirdParty imports (from github for example)
type Set struct {
	Standard   List `toml:"standard"`
	ThirdParty List `toml:"third_party"`
}

// Format the set into Go syntax (compatible with go imports)
func (s Set) Format() []byte {
	stdlen, thirdlen := len(s.Standard), len(s.ThirdParty)
	if stdlen+thirdlen < 1 {
		return []byte{}
	}

	if stdlen+thirdlen == 1 {
		var imp string
		if stdlen == 1 {
			imp = s.Standard[0]
		} else {
			imp = s.ThirdParty[0]
		}
		return []byte(fmt.Sprintf("import %s", imp))
	}

	buf := &bytes.Buffer{}
	buf.WriteString("import (")
	for _, std := range s.Standard {
		fmt.Fprintf(buf, "\n\t%s", std)
	}
	if stdlen != 0 && thirdlen != 0 {
		buf.WriteString("\n")
	}
	for _, third := range s.ThirdParty {
		fmt.Fprintf(buf, "\n\t%s", third)
	}
	buf.WriteString("\n)\n")

	return buf.Bytes()
}

// SetFromInterface creates a set from a theoretical map[string]any.
// This is to load from a loosely defined configuration file.
func SetFromInterface(intf any) (Set, error) {
	s := Set{}

	setIntf, ok := intf.(map[string]any)
	if !ok {
		return s, errors.New("import set should be map[string]any")
	}

	standardIntf, ok := setIntf["standard"]
	if ok {
		standardsIntf, ok := standardIntf.([]any)
		if !ok {
			return s, errors.New("import set standards must be an slice")
		}

		s.Standard = List{}
		for i, intf := range standardsIntf {
			str, ok := intf.(string)
			if !ok {
				return s, errors.Errorf("import set standard slice element %d (%+v) must be string", i, s)
			}
			s.Standard = append(s.Standard, str)
		}
	}

	thirdPartyIntf, ok := setIntf["third_party"]
	if ok {
		thirdPartysIntf, ok := thirdPartyIntf.([]any)
		if !ok {
			return s, errors.New("import set third_party must be an slice")
		}

		s.ThirdParty = List{}
		for i, intf := range thirdPartysIntf {
			str, ok := intf.(string)
			if !ok {
				return s, errors.Errorf("import set third party slice element %d (%+v) must be string", i, intf)
			}
			s.ThirdParty = append(s.ThirdParty, str)
		}
	}

	return s, nil
}

// Map of file/type -> imports
// Map's consumers do not understand windows paths. Always specify paths
// using forward slash (/).
type Map map[string]Set

// MapFromInterface creates a Map from a theoretical map[string]any
// or []map[string]any
// This is to load from a loosely defined configuration file.
func MapFromInterface(intf any) (Map, error) {
	m := Map{}

	iter := func(i any, fn func(string, any) error) error {
		switch toIter := intf.(type) {
		case []any:
			for _, intf := range toIter {
				obj := cast.ToStringMap(intf)
				name := obj["name"].(string)
				if err := fn(name, intf); err != nil {
					return err
				}
			}
		case map[string]any:
			for k, v := range toIter {
				if err := fn(k, v); err != nil {
					return err
				}
			}
		default:
			panic("import map should be map[string]interface or []map[string]any")
		}

		return nil
	}

	err := iter(intf, func(name string, value any) error {
		s, err := SetFromInterface(value)
		if err != nil {
			return err
		}

		m[name] = s
		return nil
	})
	if err != nil {
		return nil, err
	}

	return m, nil
}

// List of imports
type List []string

// Len implements sort.Interface.Len
func (l List) Len() int {
	return len(l)
}

// Swap implements sort.Interface.Swap
func (l List) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// Less implements sort.Interface.Less
func (l List) Less(i, j int) bool {
	res := strings.Compare(strings.TrimLeft(l[i], "_ "), strings.TrimLeft(l[j], "_ "))
	if res <= 0 {
		return true
	}

	return false
}

// NewDefaultImports returns a default Imports struct.
func NewDefaultImports() Collection {
	var col Collection

	col.All = Set{
		Standard: List{
			`"database/sql"`,
			`"fmt"`,
			`"reflect"`,
			`"strings"`,
			`"sync"`,
			`"time"`,
		},
		ThirdParty: List{
			`"github.com/friendsofgo/errors"`,
			`"github.com/aarondl/sqlboiler/v4/boil"`,
			`"github.com/aarondl/sqlboiler/v4/queries"`,
			`"github.com/aarondl/sqlboiler/v4/queries/qm"`,
			`"github.com/aarondl/sqlboiler/v4/queries/qmhelper"`,
			`"github.com/aarondl/strmangle"`,
		},
	}

	col.Singleton = Map{
		"boil_queries": {
			Standard: List{
				`"regexp"`,
			},
			ThirdParty: List{
				`"github.com/aarondl/sqlboiler/v4/drivers"`,
				`"github.com/aarondl/sqlboiler/v4/queries"`,
				`"github.com/aarondl/sqlboiler/v4/queries/qm"`,
			},
		},
		"boil_types": {
			Standard: List{
				`"strconv"`,
				`"database/sql/driver"`,
			},
			ThirdParty: List{
				`"github.com/friendsofgo/errors"`,
				`"github.com/aarondl/sqlboiler/v4/boil"`,
				`"github.com/aarondl/strmangle"`,
			},
		},
	}

	col.Test = Set{
		Standard: List{
			`"bytes"`,
			`"reflect"`,
			`"testing"`,
		},
		ThirdParty: List{
			`"github.com/aarondl/sqlboiler/v4/boil"`,
			`"github.com/aarondl/sqlboiler/v4/queries"`,
			`"github.com/aarondl/randomize"`,
			`"github.com/aarondl/strmangle"`,
		},
	}

	col.TestSingleton = Map{
		"boil_main_test": {
			Standard: List{
				`"database/sql"`,
				`"flag"`,
				`"fmt"`,
				`"math/rand"`,
				`"os"`,
				`"path/filepath"`,
				`"strings"`,
				`"testing"`,
				`"time"`,
			},
			ThirdParty: List{
				`"github.com/spf13/viper"`,
				`"github.com/aarondl/sqlboiler/v4/boil"`,
			},
		},
		"boil_queries_test": {
			Standard: List{
				`"bytes"`,
				`"fmt"`,
				`"io"`,
				`"math/rand"`,
				`"regexp"`,
			},
			ThirdParty: List{
				`"github.com/aarondl/sqlboiler/v4/boil"`,
			},
		},
		"boil_suites_test": {
			Standard: List{
				`"testing"`,
			},
		},
		"boil_relationship_test": {
			Standard: List{
				`"testing"`,
			},
		},
	}

	return col
}

// NullableEnumImports returns imports collection for nullable enum types.
func NullableEnumImports() Collection {
	var col Collection

	col.Singleton = Map{
		"boil_types": {
			Standard: List{
				`"bytes"`,
				`"database/sql/driver"`,
				`"encoding/json"`,
			},
			ThirdParty: List{
				`"github.com/aarondl/null/v8"`,
				`"github.com/aarondl/null/v8/convert"`,
			},
		},
	}

	return col
}

// AddTypeImports takes a set of imports 'a', a type -> import mapping 'typeMap'
// and a set of column types that are currently in use and produces a new set
// including both the old standard/third party, as well as the imports required
// for the types in use.
func AddTypeImports(a Set, typeMap map[string]Set, columnTypes []string) Set {
	tmpImp := Set{
		Standard:   make(List, len(a.Standard)),
		ThirdParty: make(List, len(a.ThirdParty)),
	}

	copy(tmpImp.Standard, a.Standard)
	copy(tmpImp.ThirdParty, a.ThirdParty)

	for _, typ := range columnTypes {
		for key, imp := range typeMap {
			if typ == key {
				tmpImp.Standard = append(tmpImp.Standard, imp.Standard...)
				tmpImp.ThirdParty = append(tmpImp.ThirdParty, imp.ThirdParty...)
			}
		}
	}

	tmpImp.Standard = strmangle.RemoveDuplicates(tmpImp.Standard)
	tmpImp.ThirdParty = strmangle.RemoveDuplicates(tmpImp.ThirdParty)

	sort.Sort(tmpImp.Standard)
	sort.Sort(tmpImp.ThirdParty)

	return tmpImp
}

// Merge takes two collections and creates a new one
// with the de-duplication contents of both.
func Merge(a, b Collection) Collection {
	var c Collection

	c.All = mergeSet(a.All, b.All)
	c.Test = mergeSet(a.Test, b.Test)

	c.Singleton = mergeMap(a.Singleton, b.Singleton)
	c.TestSingleton = mergeMap(a.TestSingleton, b.TestSingleton)

	c.BasedOnType = mergeMap(a.BasedOnType, b.BasedOnType)

	return c
}

func mergeSet(a, b Set) Set {
	var c Set

	c.Standard = strmangle.RemoveDuplicates(combineStringSlices(a.Standard, b.Standard))
	c.ThirdParty = strmangle.RemoveDuplicates(combineStringSlices(a.ThirdParty, b.ThirdParty))

	sort.Sort(c.Standard)
	sort.Sort(c.ThirdParty)

	return c
}

func mergeMap(a, b Map) Map {
	m := make(Map)

	for k, v := range a {
		m[k] = v
	}

	for k, toMerge := range b {
		exist, ok := m[k]
		if !ok {
			m[k] = toMerge
		}

		m[k] = mergeSet(exist, toMerge)
	}

	return m
}

func combineStringSlices(a, b []string) []string {
	c := make([]string, len(a)+len(b))
	if len(a) > 0 {
		copy(c, a)
	}
	if len(b) > 0 {
		copy(c[len(a):], b)
	}

	return c
}
