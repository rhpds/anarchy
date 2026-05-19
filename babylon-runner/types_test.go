package main

import (
	"regexp"
	"testing"
	"time"
)

func TestGetNestedMap(t *testing.T) {
	t.Run("normal nested traversal 2 levels", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": map[string]interface{}{
					"key": "value",
				},
			},
		}
		result := getNestedMap(m, "level1", "level2")
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result["key"] != "value" {
			t.Errorf("expected key=value, got %v", result["key"])
		}
	})

	t.Run("normal nested traversal 3 levels", func(t *testing.T) {
		m := map[string]interface{}{
			"a": map[string]interface{}{
				"b": map[string]interface{}{
					"c": map[string]interface{}{
						"d": "deep",
					},
				},
			},
		}
		result := getNestedMap(m, "a", "b", "c")
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result["d"] != "deep" {
			t.Errorf("expected d=deep, got %v", result["d"])
		}
	})

	t.Run("nil input map", func(t *testing.T) {
		result := getNestedMap(nil, "key")
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("empty keys", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "value",
		}
		result := getNestedMap(m)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result["key"] != "value" {
			t.Errorf("expected same map back with no keys")
		}
	})

	t.Run("single key", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": "value",
			},
		}
		result := getNestedMap(m, "level1")
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result["key"] != "value" {
			t.Errorf("expected key=value, got %v", result["key"])
		}
	})

	t.Run("missing intermediate key", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": "value",
			},
		}
		result := getNestedMap(m, "level1", "missing", "level3")
		if result != nil {
			t.Errorf("expected nil for missing intermediate key, got %v", result)
		}
	})

	t.Run("intermediate value is not a map", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": "string_not_map",
		}
		result := getNestedMap(m, "level1", "level2")
		if result != nil {
			t.Errorf("expected nil when intermediate value is not a map, got %v", result)
		}
	})

	t.Run("final key missing", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": "value",
			},
		}
		result := getNestedMap(m, "level1", "missing")
		if result != nil {
			t.Errorf("expected nil for missing final key, got %v", result)
		}
	})

	t.Run("final key value is not a map", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": "string_value",
			},
		}
		result := getNestedMap(m, "level1", "level2")
		if result != nil {
			t.Errorf("expected nil when final value is not a map, got %v", result)
		}
	})
}

func TestGetNestedString(t *testing.T) {
	t.Run("normal string retrieval 1 level", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "value",
		}
		result := getNestedString(m, "key")
		if result != "value" {
			t.Errorf("expected 'value', got %q", result)
		}
	})

	t.Run("normal string retrieval 2 levels", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": "nested_value",
			},
		}
		result := getNestedString(m, "level1", "key")
		if result != "nested_value" {
			t.Errorf("expected 'nested_value', got %q", result)
		}
	})

	t.Run("normal string retrieval 3 levels", func(t *testing.T) {
		m := map[string]interface{}{
			"a": map[string]interface{}{
				"b": map[string]interface{}{
					"c": "deep_value",
				},
			},
		}
		result := getNestedString(m, "a", "b", "c")
		if result != "deep_value" {
			t.Errorf("expected 'deep_value', got %q", result)
		}
	})

	t.Run("nil input map", func(t *testing.T) {
		result := getNestedString(nil, "key")
		if result != "" {
			t.Errorf("expected empty string, got %q", result)
		}
	})

	t.Run("empty keys", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "value",
		}
		result := getNestedString(m)
		if result != "" {
			t.Errorf("expected empty string for no keys, got %q", result)
		}
	})

	t.Run("missing key at level 1", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "value",
		}
		result := getNestedString(m, "missing")
		if result != "" {
			t.Errorf("expected empty string for missing key, got %q", result)
		}
	})

	t.Run("missing key at level 2", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": "value",
			},
		}
		result := getNestedString(m, "level1", "missing")
		if result != "" {
			t.Errorf("expected empty string for missing key, got %q", result)
		}
	})

	t.Run("missing intermediate key", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": "value",
			},
		}
		result := getNestedString(m, "missing", "key")
		if result != "" {
			t.Errorf("expected empty string for missing intermediate, got %q", result)
		}
	})

	t.Run("final value is int", func(t *testing.T) {
		m := map[string]interface{}{
			"key": 42,
		}
		result := getNestedString(m, "key")
		if result != "" {
			t.Errorf("expected empty string for int value, got %q", result)
		}
	})

	t.Run("final value is bool", func(t *testing.T) {
		m := map[string]interface{}{
			"key": true,
		}
		result := getNestedString(m, "key")
		if result != "" {
			t.Errorf("expected empty string for bool value, got %q", result)
		}
	})

	t.Run("final value is nil", func(t *testing.T) {
		m := map[string]interface{}{
			"key": nil,
		}
		result := getNestedString(m, "key")
		if result != "" {
			t.Errorf("expected empty string for nil value, got %q", result)
		}
	})

	t.Run("final value is map", func(t *testing.T) {
		m := map[string]interface{}{
			"key": map[string]interface{}{
				"nested": "value",
			},
		}
		result := getNestedString(m, "key")
		if result != "" {
			t.Errorf("expected empty string for map value, got %q", result)
		}
	})

	t.Run("intermediate value is not a map", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": "string_not_map",
		}
		result := getNestedString(m, "level1", "key")
		if result != "" {
			t.Errorf("expected empty string when intermediate is not a map, got %q", result)
		}
	})
}

func TestGetNestedBool(t *testing.T) {
	t.Run("true value at level 1", func(t *testing.T) {
		m := map[string]interface{}{
			"key": true,
		}
		result := getNestedBool(m, "key")
		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("false value at level 1", func(t *testing.T) {
		m := map[string]interface{}{
			"key": false,
		}
		result := getNestedBool(m, "key")
		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})

	t.Run("true value at level 2", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": true,
			},
		}
		result := getNestedBool(m, "level1", "key")
		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("false value at level 2", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"key": false,
			},
		}
		result := getNestedBool(m, "level1", "key")
		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})

	t.Run("nil input map", func(t *testing.T) {
		result := getNestedBool(nil, "key")
		if result != false {
			t.Errorf("expected false for nil map, got %v", result)
		}
	})

	t.Run("empty keys", func(t *testing.T) {
		m := map[string]interface{}{
			"key": true,
		}
		result := getNestedBool(m)
		if result != false {
			t.Errorf("expected false for empty keys, got %v", result)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		m := map[string]interface{}{
			"key": true,
		}
		result := getNestedBool(m, "missing")
		if result != false {
			t.Errorf("expected false for missing key, got %v", result)
		}
	})

	t.Run("final value is string", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "true",
		}
		result := getNestedBool(m, "key")
		if result != false {
			t.Errorf("expected false for string value, got %v", result)
		}
	})

	t.Run("final value is int", func(t *testing.T) {
		m := map[string]interface{}{
			"key": 1,
		}
		result := getNestedBool(m, "key")
		if result != false {
			t.Errorf("expected false for int value, got %v", result)
		}
	})

	t.Run("final value is nil", func(t *testing.T) {
		m := map[string]interface{}{
			"key": nil,
		}
		result := getNestedBool(m, "key")
		if result != false {
			t.Errorf("expected false for nil value, got %v", result)
		}
	})
}

func TestSetNested(t *testing.T) {
	t.Run("set at single level", func(t *testing.T) {
		m := make(map[string]interface{})
		setNested(m, "value", "key")
		if m["key"] != "value" {
			t.Errorf("expected key=value, got %v", m["key"])
		}
	})

	t.Run("set at 2 levels creates intermediate", func(t *testing.T) {
		m := make(map[string]interface{})
		setNested(m, "value", "level1", "key")
		level1, ok := m["level1"].(map[string]interface{})
		if !ok {
			t.Fatal("expected level1 to be a map")
		}
		if level1["key"] != "value" {
			t.Errorf("expected key=value, got %v", level1["key"])
		}
	})

	t.Run("set at 3 levels creates intermediates", func(t *testing.T) {
		m := make(map[string]interface{})
		setNested(m, "deep_value", "a", "b", "c")
		a, ok := m["a"].(map[string]interface{})
		if !ok {
			t.Fatal("expected a to be a map")
		}
		b, ok := a["b"].(map[string]interface{})
		if !ok {
			t.Fatal("expected b to be a map")
		}
		if b["c"] != "deep_value" {
			t.Errorf("expected c=deep_value, got %v", b["c"])
		}
	})

	t.Run("overwrite existing value", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "old_value",
		}
		setNested(m, "new_value", "key")
		if m["key"] != "new_value" {
			t.Errorf("expected key=new_value, got %v", m["key"])
		}
	})

	t.Run("overwrite non-map intermediate", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": "string_not_map",
		}
		setNested(m, "value", "level1", "key")
		level1, ok := m["level1"].(map[string]interface{})
		if !ok {
			t.Fatal("expected level1 to be replaced with a map")
		}
		if level1["key"] != "value" {
			t.Errorf("expected key=value, got %v", level1["key"])
		}
	})

	t.Run("empty keys is no-op", func(t *testing.T) {
		m := map[string]interface{}{
			"key": "value",
		}
		original := m["key"]
		setNested(m, "new_value")
		if m["key"] != original {
			t.Errorf("expected map unchanged with empty keys")
		}
	})

	t.Run("nil map panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on nil map, but did not panic")
			}
		}()
		var m map[string]interface{}
		setNested(m, "value", "key")
	})

	t.Run("set with partial existing path", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"existing": "data",
			},
		}
		setNested(m, "value", "level1", "new_key")
		level1, ok := m["level1"].(map[string]interface{})
		if !ok {
			t.Fatal("expected level1 to be a map")
		}
		if level1["new_key"] != "value" {
			t.Errorf("expected new_key=value, got %v", level1["new_key"])
		}
		if level1["existing"] != "data" {
			t.Errorf("expected existing data to remain, got %v", level1["existing"])
		}
	})
}

func TestMergeMap(t *testing.T) {
	t.Run("normal merge with new keys", func(t *testing.T) {
		dst := map[string]interface{}{
			"key1": "value1",
		}
		src := map[string]interface{}{
			"key2": "value2",
		}
		mergeMap(dst, src)
		if dst["key1"] != "value1" {
			t.Errorf("expected key1=value1, got %v", dst["key1"])
		}
		if dst["key2"] != "value2" {
			t.Errorf("expected key2=value2, got %v", dst["key2"])
		}
	})

	t.Run("overwrite existing keys", func(t *testing.T) {
		dst := map[string]interface{}{
			"key": "old_value",
		}
		src := map[string]interface{}{
			"key": "new_value",
		}
		mergeMap(dst, src)
		if dst["key"] != "new_value" {
			t.Errorf("expected key=new_value, got %v", dst["key"])
		}
	})

	t.Run("empty src no change", func(t *testing.T) {
		dst := map[string]interface{}{
			"key": "value",
		}
		src := map[string]interface{}{}
		mergeMap(dst, src)
		if dst["key"] != "value" {
			t.Errorf("expected key=value, got %v", dst["key"])
		}
		if len(dst) != 1 {
			t.Errorf("expected dst to have 1 key, got %d", len(dst))
		}
	})

	t.Run("empty dst copies all", func(t *testing.T) {
		dst := map[string]interface{}{}
		src := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}
		mergeMap(dst, src)
		if dst["key1"] != "value1" {
			t.Errorf("expected key1=value1, got %v", dst["key1"])
		}
		if dst["key2"] != "value2" {
			t.Errorf("expected key2=value2, got %v", dst["key2"])
		}
	})

	t.Run("both empty", func(t *testing.T) {
		dst := map[string]interface{}{}
		src := map[string]interface{}{}
		mergeMap(dst, src)
		if len(dst) != 0 {
			t.Errorf("expected empty dst, got %d keys", len(dst))
		}
	})

	t.Run("shallow merge does not deep copy", func(t *testing.T) {
		nested := map[string]interface{}{
			"nested_key": "nested_value",
		}
		dst := map[string]interface{}{}
		src := map[string]interface{}{
			"key": nested,
		}
		mergeMap(dst, src)
		// Verify it's a shallow copy by checking reference equality
		dstNested, ok := dst["key"].(map[string]interface{})
		if !ok {
			t.Fatal("expected nested map in dst")
		}
		// Modify the original nested map
		nested["nested_key"] = "modified"
		// The dst should see the change (shallow copy)
		if dstNested["nested_key"] != "modified" {
			t.Errorf("expected shallow copy - dst should see modifications to src nested map")
		}
	})
}

func TestNowUTC(t *testing.T) {
	t.Run("format matches RFC3339 pattern", func(t *testing.T) {
		result := nowUTC()
		// RFC3339 without fractional seconds: 2006-01-02T15:04:05Z
		pattern := `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`
		matched, err := regexp.MatchString(pattern, result)
		if err != nil {
			t.Fatalf("regex error: %v", err)
		}
		if !matched {
			t.Errorf("expected RFC3339 format, got %q", result)
		}
	})

	t.Run("ends with Z", func(t *testing.T) {
		result := nowUTC()
		if result[len(result)-1] != 'Z' {
			t.Errorf("expected to end with 'Z', got %q", result)
		}
	})

	t.Run("length is 20 characters", func(t *testing.T) {
		result := nowUTC()
		if len(result) != 20 {
			t.Errorf("expected length 20, got %d: %q", len(result), result)
		}
	})

	t.Run("is parseable as time", func(t *testing.T) {
		result := nowUTC()
		parsed, err := time.Parse("2006-01-02T15:04:05Z", result)
		if err != nil {
			t.Errorf("failed to parse result as time: %v", err)
		}
		// Verify it's close to now (within 1 second)
		now := time.Now().UTC()
		diff := now.Sub(parsed)
		if diff < 0 {
			diff = -diff
		}
		if diff > time.Second {
			t.Errorf("time difference too large: %v", diff)
		}
	})

	t.Run("is in UTC timezone", func(t *testing.T) {
		result := nowUTC()
		parsed, err := time.Parse(time.RFC3339, result)
		if err != nil {
			t.Errorf("failed to parse: %v", err)
		}
		loc := parsed.Location()
		if loc != time.UTC {
			t.Errorf("expected UTC timezone, got %v", loc)
		}
	})
}
