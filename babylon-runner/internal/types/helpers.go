package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// GetNestedMap safely traverses nested maps by keys.
// Returns nil if any key is missing or the value is not a map.
func GetNestedMap(m map[string]interface{}, keys ...string) map[string]interface{} {
	current := m
	for _, k := range keys {
		next, ok := current[k].(map[string]interface{})
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// GetNestedString safely extracts a string from nested maps.
// Returns "" if the path is missing or the value is not a string.
func GetNestedString(m map[string]interface{}, keys ...string) string {
	if len(keys) == 0 {
		return ""
	}
	parent := GetNestedMap(m, keys[:len(keys)-1]...)
	if parent == nil {
		return ""
	}
	s, _ := parent[keys[len(keys)-1]].(string)
	return s
}

// GetNestedBool safely extracts a bool from nested maps.
// Returns false if the path is missing or the value is not a bool.
func GetNestedBool(m map[string]interface{}, keys ...string) bool {
	if len(keys) == 0 {
		return false
	}
	parent := GetNestedMap(m, keys[:len(keys)-1]...)
	if parent == nil {
		return false
	}
	b, _ := parent[keys[len(keys)-1]].(bool)
	return b
}

// SetNested sets a value at the given key path in a nested map,
// creating intermediate maps as needed.
func SetNested(m map[string]interface{}, value interface{}, keys ...string) {
	if len(keys) == 0 {
		return
	}
	current := m
	for _, k := range keys[:len(keys)-1] {
		next, ok := current[k].(map[string]interface{})
		if !ok {
			next = make(map[string]interface{})
			current[k] = next
		}
		current = next
	}
	current[keys[len(keys)-1]] = value
}

// NowUTC returns the current time as an RFC3339 string in UTC.
func NowUTC() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// DeepMergeMap recursively merges src into dst. When both dst[k] and src[k]
// are map[string]interface{}, their contents are merged recursively.
// Otherwise src[k] overwrites dst[k].
func DeepMergeMap(dst, src map[string]interface{}) {
	for k, v := range src {
		if srcMap, ok := v.(map[string]interface{}); ok {
			if dstMap, ok := dst[k].(map[string]interface{}); ok {
				DeepMergeMap(dstMap, srcMap)
				continue
			}
		}
		dst[k] = v
	}
}

// ExtractStringSlice extracts a []string from a map value that may be
// []interface{} (as produced by JSON unmarshaling).
func ExtractStringSlice(m map[string]interface{}, key string) []string {
	raw, ok := m[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	default:
		return nil
	}
}

// AfterTimestamp converts a Go duration string (e.g., "5m", "1h") to an
// absolute RFC3339 timestamp. If the duration string is already an absolute
// timestamp (contains "T"), it is returned as-is. Returns the current time
// on parse error.
func AfterTimestamp(after string) string {
	if after == "" {
		return NowUTC()
	}
	for _, ch := range after {
		if ch == 'T' {
			return after
		}
	}
	d, err := time.ParseDuration(after)
	if err != nil {
		return NowUTC()
	}
	return time.Now().UTC().Add(d).Format(time.RFC3339)
}

// StringFromMap extracts a string value from a map, returning "" if missing
// or not a string.
func StringFromMap(m map[string]interface{}, key string) string {
	s, _ := m[key].(string)
	return s
}

// FloatFromMap extracts a float64 value from a map, returning 0 if missing.
func FloatFromMap(m map[string]interface{}, key string) float64 {
	f, _ := m[key].(float64)
	return f
}

// FirstString returns the first non-empty string from the arguments.
func FirstString(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// DeepCopyMap returns a deep copy of a map[string]interface{}.
func DeepCopyMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[k] = DeepCopyMap(val)
		case []interface{}:
			dst[k] = DeepCopySlice(val)
		default:
			dst[k] = v
		}
	}
	return dst
}

// DeepCopySlice returns a deep copy of a []interface{}.
func DeepCopySlice(src []interface{}) []interface{} {
	if src == nil {
		return nil
	}
	dst := make([]interface{}, len(src))
	for i, v := range src {
		switch val := v.(type) {
		case map[string]interface{}:
			dst[i] = DeepCopyMap(val)
		case []interface{}:
			dst[i] = DeepCopySlice(val)
		default:
			dst[i] = v
		}
	}
	return dst
}

// StringOrSlice holds label values that may arrive as a single string
// or a list of strings in JSON/YAML. It always normalises to []string.
type StringOrSlice []string

func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = []string{single}
		return nil
	}
	var slice []string
	if err := json.Unmarshal(data, &slice); err != nil {
		return err
	}
	*s = slice
	return nil
}

// FormatTimestamp returns a formatted timestamp string.
// Used for startTimestamp fields.
func FormatTimestamp() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}
