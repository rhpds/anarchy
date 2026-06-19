package types

import (
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

func TestMergeMap(t *testing.T) {
	dst := map[string]interface{}{"a": 1, "b": 2}
	src := map[string]interface{}{"b": 3, "c": 4}
	MergeMap(dst, src)
	if dst["a"] != 1 || dst["b"] != 3 || dst["c"] != 4 {
		t.Errorf("unexpected merge result: %v", dst)
	}
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
