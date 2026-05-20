package main

import (
	"fmt"
	"regexp"
	"strings"
)

// j2ExprRe matches a single {{ expression }} pattern.
var j2ExprRe = regexp.MustCompile(`\{\{\s*(.+?)\s*\}\}`)

// resolveJ2 recursively walks data and resolves Jinja2 {{ }} expressions
// in string values using the provided variable context.
//
// This handles the subset of Jinja2 used in governor __meta__:
//   - {{ var }} or {{ dotted.path }}
//   - {{ var | default('value') }}
//
// When a string is entirely a single {{ expr }}, the resolved value
// retains its original type (e.g. bool, int). When {{ expr }} is
// embedded in a larger string, the resolved value is stringified.
func resolveJ2(data interface{}, vars map[string]interface{}) interface{} {
	switch v := data.(type) {
	case string:
		return resolveJ2String(v, vars)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, val := range v {
			out[k] = resolveJ2(val, vars)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, val := range v {
			out[i] = resolveJ2(val, vars)
		}
		return out
	default:
		return data
	}
}

// resolveJ2String resolves {{ }} expressions within a string.
func resolveJ2String(s string, vars map[string]interface{}) interface{} {
	trimmed := strings.TrimSpace(s)

	// Fast path: no Jinja2.
	if !strings.Contains(trimmed, "{{") {
		return s
	}

	// If the entire string is one {{ expr }}, return the typed value.
	if strings.HasPrefix(trimmed, "{{") && strings.HasSuffix(trimmed, "}}") {
		inner := trimmed[2 : len(trimmed)-2]
		// Make sure there's no nested {{ inside.
		if !strings.Contains(inner, "{{") {
			if val, ok := evalJ2Expr(strings.TrimSpace(inner), vars); ok {
				return val
			}
			return s
		}
	}

	// Inline: replace each {{ expr }} with its string representation.
	return j2ExprRe.ReplaceAllStringFunc(s, func(match string) string {
		inner := strings.TrimSpace(match[2 : len(match)-2])
		if val, ok := evalJ2Expr(inner, vars); ok {
			return fmt.Sprintf("%v", val)
		}
		return match
	})
}

// evalJ2Expr evaluates "varpath" or "varpath | default('val')".
func evalJ2Expr(expr string, vars map[string]interface{}) (interface{}, bool) {
	parts := strings.SplitN(expr, "|", 2)
	varPath := strings.TrimSpace(parts[0])

	val, found := lookupDotted(varPath, vars)

	if len(parts) > 1 {
		filter := strings.TrimSpace(parts[1])
		if strings.HasPrefix(filter, "default(") {
			if !found || val == nil || val == "" {
				return extractDefaultArg(filter), true
			}
		}
	}

	if found {
		return val, true
	}
	return nil, false
}

// lookupDotted follows a dotted path (e.g. "job_vars.namespace_suffix")
// through nested maps.
func lookupDotted(path string, vars map[string]interface{}) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = vars
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// extractDefaultArg extracts the argument from default('value') or default("value").
// Unquoted values are returned as-is (handles booleans/numbers as strings).
func extractDefaultArg(filter string) string {
	// filter looks like: default('value') or default("value")
	start := strings.Index(filter, "(")
	end := strings.LastIndex(filter, ")")
	if start < 0 || end <= start {
		return ""
	}
	arg := strings.TrimSpace(filter[start+1 : end])

	// Strip surrounding quotes.
	if len(arg) >= 2 {
		if (arg[0] == '\'' && arg[len(arg)-1] == '\'') ||
			(arg[0] == '"' && arg[len(arg)-1] == '"') {
			return arg[1 : len(arg)-1]
		}
	}
	return arg
}

// j2VarContext builds the Jinja2 variable context from subject and
// governor vars — the same flat namespace the Ansible runner uses.
func j2VarContext(rc *RunContext) map[string]interface{} {
	ctx := make(map[string]interface{})
	// Subject vars (lower priority).
	if sv := getNestedMap(rc.Payload.Subject, "spec", "vars"); sv != nil {
		mergeMap(ctx, sv)
	}
	// Governor vars override subject.
	if gv := getNestedMap(rc.Payload.Governor, "spec", "vars"); gv != nil {
		mergeMap(ctx, gv)
	}
	return ctx
}
