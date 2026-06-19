package template

import (
	"testing"
)

func TestResolveJ2SimpleVar(t *testing.T) {
	vars := map[string]interface{}{
		"name": "hello",
	}
	got := ResolveJ2("{{ name }}", vars)
	if got != "hello" {
		t.Errorf("got %v, want hello", got)
	}
}

func TestResolveJ2DottedPath(t *testing.T) {
	vars := map[string]interface{}{
		"job_vars": map[string]interface{}{
			"namespace_suffix": "staging",
		},
	}
	got := ResolveJ2("{{ job_vars.namespace_suffix }}", vars)
	if got != "staging" {
		t.Errorf("got %v, want staging", got)
	}
}

func TestResolveJ2DefaultUsed(t *testing.T) {
	vars := map[string]interface{}{
		"job_vars": map[string]interface{}{},
	}
	got := ResolveJ2("{{ job_vars.namespace_suffix | default('dev') }}", vars)
	if got != "dev" {
		t.Errorf("got %v, want dev", got)
	}
}

func TestResolveJ2DefaultNotUsed(t *testing.T) {
	vars := map[string]interface{}{
		"job_vars": map[string]interface{}{
			"namespace_suffix": "prod",
		},
	}
	got := ResolveJ2("{{ job_vars.namespace_suffix | default('dev') }}", vars)
	if got != "prod" {
		t.Errorf("got %v, want prod", got)
	}
}

func TestResolveJ2MissingVarNoDefault(t *testing.T) {
	vars := map[string]interface{}{}
	got := ResolveJ2("{{ missing }}", vars)
	// Unresolvable: return original string.
	if got != "{{ missing }}" {
		t.Errorf("got %v, want original expression", got)
	}
}

func TestResolveJ2NestedMap(t *testing.T) {
	vars := map[string]interface{}{
		"job_vars": map[string]interface{}{
			"namespace_suffix":    "dev",
			"param_selector_virt": "yes",
		},
	}
	data := map[string]interface{}{
		"kind":             "OcpSandbox",
		"namespace_suffix": "{{ job_vars.namespace_suffix | default('dev') }}",
		"cloud_selector": map[string]interface{}{
			"cloud":    "cnv-shared",
			"purpose":  "prod",
			"keycloak": "yes",
			"virt":     "{{ job_vars.param_selector_virt | default('no') }}",
		},
	}
	result := ResolveJ2(data, vars).(map[string]interface{})

	if result["kind"] != "OcpSandbox" {
		t.Errorf("kind = %v, want OcpSandbox", result["kind"])
	}
	if result["namespace_suffix"] != "dev" {
		t.Errorf("namespace_suffix = %v, want dev", result["namespace_suffix"])
	}
	cs := result["cloud_selector"].(map[string]interface{})
	if cs["virt"] != "yes" {
		t.Errorf("virt = %v, want yes", cs["virt"])
	}
	if cs["cloud"] != "cnv-shared" {
		t.Errorf("cloud = %v, want cnv-shared", cs["cloud"])
	}
}

func TestResolveJ2Slice(t *testing.T) {
	vars := map[string]interface{}{
		"job_vars": map[string]interface{}{
			"ns": "test",
		},
	}
	data := []interface{}{
		map[string]interface{}{
			"kind":             "OcpSandbox",
			"namespace_suffix": "{{ job_vars.ns | default('dev') }}",
		},
	}
	result := ResolveJ2(data, vars).([]interface{})
	entry := result[0].(map[string]interface{})
	if entry["namespace_suffix"] != "test" {
		t.Errorf("namespace_suffix = %v, want test", entry["namespace_suffix"])
	}
}

func TestResolveJ2InlineExpression(t *testing.T) {
	vars := map[string]interface{}{
		"name": "world",
	}
	got := ResolveJ2("hello {{ name }}!", vars)
	if got != "hello world!" {
		t.Errorf("got %v, want 'hello world!'", got)
	}
}

func TestResolveJ2NoExpression(t *testing.T) {
	got := ResolveJ2("plain string", nil)
	if got != "plain string" {
		t.Errorf("got %v, want plain string", got)
	}
}

func TestResolveJ2DoubleQuotedDefault(t *testing.T) {
	vars := map[string]interface{}{}
	got := ResolveJ2(`{{ x | default("fallback") }}`, vars)
	if got != "fallback" {
		t.Errorf("got %v, want fallback", got)
	}
}

func TestResolveJ2EmptyStringTriggersDefault(t *testing.T) {
	vars := map[string]interface{}{
		"val": "",
	}
	got := ResolveJ2("{{ val | default('fallback') }}", vars)
	if got != "fallback" {
		t.Errorf("got %v, want fallback", got)
	}
}

func TestResolveJ2NonStringPassthrough(t *testing.T) {
	got := ResolveJ2(42, nil)
	if got != 42 {
		t.Errorf("got %v, want 42", got)
	}
}

func TestJ2VarContext(t *testing.T) {
	subjectAllVars := map[string]interface{}{
		"current_state": "started",
		"job_vars": map[string]interface{}{
			"uuid": "abc",
			"ns":   "from-subject",
		},
		"provision_data": map[string]interface{}{
			"console": "url",
		},
	}
	govAllVars := map[string]interface{}{
		"job_vars": map[string]interface{}{
			"env_type": "ocp4-demo",
			"region":   "us-east-1",
		},
		"__meta__": map[string]interface{}{
			"aws_sandboxed": true,
		},
	}

	vars := J2VarContext(subjectAllVars, govAllVars)

	// Governor overrides subject for job_vars key.
	jv, ok := vars["job_vars"].(map[string]interface{})
	if !ok {
		t.Fatal("job_vars missing from context")
	}
	if jv["env_type"] != "ocp4-demo" {
		t.Errorf("env_type = %v, want ocp4-demo", jv["env_type"])
	}
	if jv["region"] != "us-east-1" {
		t.Errorf("region = %v, want us-east-1", jv["region"])
	}

	// Subject-only keys preserved.
	if vars["current_state"] != "started" {
		t.Errorf("current_state = %v, want started", vars["current_state"])
	}
	pd, ok := vars["provision_data"].(map[string]interface{})
	if !ok {
		t.Fatal("provision_data missing from context")
	}
	if pd["console"] != "url" {
		t.Errorf("provision_data.console = %v, want url", pd["console"])
	}

	// Governor-only keys preserved.
	meta, ok := vars["__meta__"].(map[string]interface{})
	if !ok {
		t.Fatal("__meta__ missing from context")
	}
	if meta["aws_sandboxed"] != true {
		t.Errorf("__meta__.aws_sandboxed = %v, want true", meta["aws_sandboxed"])
	}
}
