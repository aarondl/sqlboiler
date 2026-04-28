package boilingcore

import (
	"sort"
	"testing"
	"text/template"
)

func TestTemplateNameListSort(t *testing.T) {
	t.Parallel()

	templs := templateNameList{
		"bob.tpl",
		"all.tpl",
		"struct.tpl",
		"ttt.tpl",
	}

	expected := []string{"bob.tpl", "all.tpl", "struct.tpl", "ttt.tpl"}

	for i, v := range templs {
		if v != expected[i] {
			t.Errorf("Order mismatch, expected: %s, got: %s", expected[i], v)
		}
	}

	expected = []string{"struct.tpl", "all.tpl", "bob.tpl", "ttt.tpl"}

	sort.Sort(templs)

	for i, v := range templs {
		if v != expected[i] {
			t.Errorf("Order mismatch, expected: %s, got: %s", expected[i], v)
		}
	}
}

func TestCamelCaseNoInitialisms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"account_id", "accountId"},
		{"user_name", "userName"},
		{"http_url", "httpUrl"},
		{"first_last_name", "firstLastName"},
		{"id", "id"},
		{"column_name_id", "columnNameId"},
		{"", ""},
		{"single", "single"},
		{"a_b_c", "aBC"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := camelCaseNoInitialisms(tt.input)
			if got != tt.expected {
				t.Errorf("camelCaseNoInitialisms(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGenerateTagWithCaseCamel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"account_id", `json:"accountId" `},
		{"user_name", `json:"userName" `},
		{"http_url", `json:"httpUrl" `},
		{"id", `json:"id" `},
		{"column_name_id", `json:"columnNameId" `},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := generateTagWithCase("json", tt.input, tt.input, TagCaseCamel, false)
			if got != tt.expected {
				t.Errorf("generateTagWithCase(%q, camel) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestTemplateList_Templates(t *testing.T) {
	t.Parallel()

	tpl := template.New("")
	tpl.New("wat.tpl").Parse("hello")
	tpl.New("que.tpl").Parse("there")
	tpl.New("not").Parse("hello")

	tplList := templateList{tpl}
	foundWat, foundQue, foundNot := false, false, false
	for _, n := range tplList.Templates() {
		switch n {
		case "wat.tpl":
			foundWat = true
		case "que.tpl":
			foundQue = true
		case "not":
			foundNot = true
		}
	}

	if !foundWat {
		t.Error("want wat")
	}
	if !foundQue {
		t.Error("want que")
	}
	if foundNot {
		t.Error("don't want not")
	}
}
