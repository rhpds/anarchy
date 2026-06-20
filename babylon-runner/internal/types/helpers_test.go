package types

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestGetNestedMap(t *testing.T) {
	m := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"key": "value",
			},
		},
	}

	t.Run("valid path", func(t *testing.T) {
		result := GetNestedMap(m, "level1", "level2")
		if result == nil {
			t.Fatal("expected non-nil map")
		}
		if result["key"] != "value" {
			t.Errorf("got %v, want %q", result["key"], "value")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		result := GetNestedMap(m, "level1", "missing")
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("nil map", func(t *testing.T) {
		result := GetNestedMap(nil, "key")
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}

func TestGetNestedString(t *testing.T) {
	m := map[string]interface{}{
		"spec": map[string]interface{}{
			"vars": map[string]interface{}{
				"current_state": "started",
			},
		},
	}

	if got := GetNestedString(m, "spec", "vars", "current_state"); got != "started" {
		t.Errorf("got %q, want %q", got, "started")
	}
	if got := GetNestedString(m, "spec", "vars", "missing"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestGetNestedBool(t *testing.T) {
	m := map[string]interface{}{
		"config": map[string]interface{}{
			"enabled": true,
		},
	}
	if got := GetNestedBool(m, "config", "enabled"); !got {
		t.Error("expected true")
	}
	if got := GetNestedBool(m, "config", "missing"); got {
		t.Error("expected false for missing key")
	}
}

func TestSetNested(t *testing.T) {
	m := make(map[string]interface{})
	SetNested(m, "value", "a", "b", "c")
	if got := GetNestedString(m, "a", "b", "c"); got != "value" {
		t.Errorf("got %q, want %q", got, "value")
	}
}

func TestDeepMergeMap(t *testing.T) {
	t.Run("shallow keys", func(t *testing.T) {
		dst := map[string]interface{}{"a": 1, "b": 2}
		src := map[string]interface{}{"b": 3, "c": 4}
		DeepMergeMap(dst, src)
		if dst["a"] != 1 || dst["b"] != 3 || dst["c"] != 4 {
			t.Errorf("unexpected result: %v", dst)
		}
	})

	t.Run("nested map merge", func(t *testing.T) {
		dst := map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{"timeout": 300},
				"sandbox":  "keep-this",
			},
		}
		src := map[string]interface{}{
			"__meta__": map[string]interface{}{
				"deployer": map[string]interface{}{"retries": 3},
				"new_key":  "added",
			},
		}
		DeepMergeMap(dst, src)
		meta := dst["__meta__"].(map[string]interface{})
		deployer := meta["deployer"].(map[string]interface{})
		if deployer["timeout"] != 300 {
			t.Error("deep merge lost existing nested key 'timeout'")
		}
		if deployer["retries"] != 3 {
			t.Error("deep merge did not add 'retries'")
		}
		if meta["sandbox"] != "keep-this" {
			t.Error("deep merge lost sibling key 'sandbox'")
		}
		if meta["new_key"] != "added" {
			t.Error("deep merge did not add 'new_key'")
		}
	})

	t.Run("src overwrites non-map with map", func(t *testing.T) {
		dst := map[string]interface{}{"key": "string-value"}
		src := map[string]interface{}{"key": map[string]interface{}{"nested": true}}
		DeepMergeMap(dst, src)
		if _, ok := dst["key"].(map[string]interface{}); !ok {
			t.Error("expected map to replace string")
		}
	})

	t.Run("src overwrites map with non-map", func(t *testing.T) {
		dst := map[string]interface{}{"key": map[string]interface{}{"nested": true}}
		src := map[string]interface{}{"key": "string-value"}
		DeepMergeMap(dst, src)
		if dst["key"] != "string-value" {
			t.Errorf("expected string, got %v", dst["key"])
		}
	})

	t.Run("nil src value", func(t *testing.T) {
		dst := map[string]interface{}{"a": 1}
		src := map[string]interface{}{"a": nil}
		DeepMergeMap(dst, src)
		if dst["a"] != nil {
			t.Errorf("expected nil, got %v", dst["a"])
		}
	})
}

func TestExtractStringSlice(t *testing.T) {
	m := map[string]interface{}{
		"tags": []interface{}{"a", "b", "c"},
	}
	got := ExtractStringSlice(m, "tags")
	if len(got) != 3 || got[0] != "a" {
		t.Errorf("got %v, want [a b c]", got)
	}
}

func TestNowUTC(t *testing.T) {
	ts := NowUTC()
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatalf("invalid RFC3339: %v", err)
	}
	if parsed.Location() != time.UTC {
		t.Error("not UTC")
	}
}

func TestAfterTimestamp(t *testing.T) {
	t.Run("duration string", func(t *testing.T) {
		ts := AfterTimestamp("5m")
		parsed, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
		diff := time.Until(parsed)
		if diff < 4*time.Minute || diff > 6*time.Minute {
			t.Errorf("expected ~5m from now, got %v", diff)
		}
	})

	t.Run("empty string", func(t *testing.T) {
		ts := AfterTimestamp("")
		if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
	})

	t.Run("zero duration", func(t *testing.T) {
		ts := AfterTimestamp("0s")
		if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
	})

	t.Run("invalid duration logs warning", func(t *testing.T) {
		ts := AfterTimestamp("notaduration")
		if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Fatalf("invalid RFC3339: %v", err)
		}
	})
}

func TestDeepCopyMap(t *testing.T) {
	src := map[string]interface{}{
		"nested": map[string]interface{}{"key": "val"},
		"list":   []interface{}{1, 2, 3},
	}
	dst := DeepCopyMap(src)
	dst["nested"].(map[string]interface{})["key"] = "changed"
	if src["nested"].(map[string]interface{})["key"] != "val" {
		t.Error("deep copy is not deep — original was mutated")
	}
}

func TestFirstString(t *testing.T) {
	if got := FirstString("", "", "hello"); got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
	if got := FirstString("first", "second"); got != "first" {
		t.Errorf("got %q, want %q", got, "first")
	}
}

func TestStringOrSlice(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    StringOrSlice
		wantErr bool
	}{
		{
			name:  "single string",
			input: `"prod"`,
			want:  StringOrSlice{"prod"},
		},
		{
			name:  "string slice",
			input: `["us-east","us-west"]`,
			want:  StringOrSlice{"us-east", "us-west"},
		},
		{
			name:  "single element slice",
			input: `["prod"]`,
			want:  StringOrSlice{"prod"},
		},
		{
			name:  "empty slice",
			input: `[]`,
			want:  StringOrSlice{},
		},
		{
			name:    "invalid type",
			input:   `123`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got StringOrSlice
			err := json.Unmarshal([]byte(tt.input), &got)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringOrSliceInMap(t *testing.T) {
	input := `{"env":"prod","region":["us-east","us-west"]}`
	var labels map[string]StringOrSlice
	if err := json.Unmarshal([]byte(input), &labels); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(labels["env"], StringOrSlice{"prod"}) {
		t.Errorf("env = %v, want [prod]", labels["env"])
	}
	if !reflect.DeepEqual(labels["region"], StringOrSlice{"us-east", "us-west"}) {
		t.Errorf("region = %v, want [us-east us-west]", labels["region"])
	}

	out, err := json.Marshal(labels)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	expected := `{"env":["prod"],"region":["us-east","us-west"]}`
	if string(out) != expected {
		t.Errorf("marshal = %s, want %s", out, expected)
	}
}
