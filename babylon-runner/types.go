package main

import (
	"time"
)

// RunPayload is the response from GET /run.
type RunPayload struct {
	Handler  Handler                `json:"handler"`
	Governor map[string]interface{} `json:"governor"`
	Subject  map[string]interface{} `json:"subject"`
	Action   map[string]interface{} `json:"action,omitempty"`
	Run      map[string]interface{} `json:"run"`
}

// Handler describes the handler type and parameters.
type Handler struct {
	Type string                 `json:"type"` // "action", "actionCallback", "subjectEvent"
	Name string                 `json:"name,omitempty"`
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// RunResult is the body for POST /run/{name}.
type RunResult struct {
	Result ResultPayload `json:"result"`
}

// ResultPayload holds the outcome of a handler execution.
type ResultPayload struct {
	RC            int    `json:"rc"`
	Status        string `json:"status"` // "successful" or "failed"
	StatusMessage string `json:"statusMessage,omitempty"`
}

// SubjectPatch is the body for PATCH /run/subject/{name}.
type SubjectPatch struct {
	Patch PatchBody `json:"patch"`
}

// PatchBody describes the fields that can be patched on a subject.
type PatchBody struct {
	Metadata             *PatchMetadata         `json:"metadata,omitempty"`
	Spec                 *PatchSpec             `json:"spec,omitempty"`
	Status               map[string]interface{} `json:"status,omitempty"`
	SkipUpdateProcessing bool                   `json:"skip_update_processing,omitempty"`
}

// PatchMetadata holds label and annotation patches.
type PatchMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PatchSpec holds spec-level patches.
type PatchSpec struct {
	Vars map[string]interface{} `json:"vars,omitempty"`
}

// ScheduleActionRequest is the body for POST /run/subject/{name}/actions.
type ScheduleActionRequest struct {
	Action string                 `json:"action"`
	After  string                 `json:"after,omitempty"`
	Cancel []string               `json:"cancel,omitempty"`
	Vars   map[string]interface{} `json:"vars,omitempty"`
}

// getNestedMap safely traverses nested maps and returns the map at the given
// key path. Returns nil if any key is missing or a value is not a map.
func getNestedMap(m map[string]interface{}, keys ...string) map[string]interface{} {
	current := m
	for _, key := range keys {
		val, ok := current[key]
		if !ok {
			return nil
		}
		next, ok := val.(map[string]interface{})
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// getNestedString safely traverses nested maps and returns the string value
// at the final key. Returns "" if any key is missing or the final value is
// not a string.
func getNestedString(m map[string]interface{}, keys ...string) string {
	if len(keys) == 0 {
		return ""
	}
	parent := m
	if len(keys) > 1 {
		parent = getNestedMap(m, keys[:len(keys)-1]...)
		if parent == nil {
			return ""
		}
	}
	val, ok := parent[keys[len(keys)-1]]
	if !ok {
		return ""
	}
	s, ok := val.(string)
	if !ok {
		return ""
	}
	return s
}

// getNestedBool safely traverses nested maps and returns the bool value
// at the final key. Returns false if any key is missing or the final value
// is not a bool.
func getNestedBool(m map[string]interface{}, keys ...string) bool {
	if len(keys) == 0 {
		return false
	}
	parent := m
	if len(keys) > 1 {
		parent = getNestedMap(m, keys[:len(keys)-1]...)
		if parent == nil {
			return false
		}
	}
	val, ok := parent[keys[len(keys)-1]]
	if !ok {
		return false
	}
	b, ok := val.(bool)
	if !ok {
		return false
	}
	return b
}

// setNested sets a value at the given key path, creating intermediate maps
// as needed. The last element in keys is the key to set; all preceding
// elements are map keys to traverse or create.
func setNested(m map[string]interface{}, value interface{}, keys ...string) {
	if len(keys) == 0 {
		return
	}
	current := m
	for _, key := range keys[:len(keys)-1] {
		val, ok := current[key]
		if !ok {
			next := make(map[string]interface{})
			current[key] = next
			current = next
			continue
		}
		next, ok := val.(map[string]interface{})
		if !ok {
			next = make(map[string]interface{})
			current[key] = next
			current = next
			continue
		}
		current = next
	}
	current[keys[len(keys)-1]] = value
}

// nowUTC returns the current time formatted as an RFC3339 UTC string
// without fractional seconds.
func nowUTC() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
}

// mergeMap performs a shallow merge of src into dst. Keys in src overwrite
// keys in dst.
func mergeMap(dst, src map[string]interface{}) {
	for k, v := range src {
		dst[k] = v
	}
}
