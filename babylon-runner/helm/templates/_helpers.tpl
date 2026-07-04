{{/*
babylon-runner/helm/templates/_helpers.tpl
Helper templates for babylon-runner Helm chart
*/}}

{{- define "babylon-runner.name" -}}
{{-   default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "babylon-runner.fullname" -}}
{{-   if .Values.fullnameOverride -}}
{{-     .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{-   else -}}
{{-     $name := default .Chart.Name .Values.nameOverride -}}
{{-     if contains $name .Release.Name -}}
{{-       .Release.Name | trunc 63 | trimSuffix "-" -}}
{{-     else -}}
{{-       printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{-     end -}}
{{-   end -}}
{{- end -}}

{{- define "babylon-runner.chart" -}}
{{-   printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "babylon-runner.labels" -}}
helm.sh/chart: {{ include "babylon-runner.chart" . }}
{{ include "babylon-runner.selectorLabels" . }}
{{-   if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{-   end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "babylon-runner.selectorLabels" -}}
app.kubernetes.io/name: {{ include "babylon-runner.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "babylon-runner.namespaceName" -}}
{{-   if .Values.namespace.create -}}
{{      default (include "babylon-runner.name" .) .Values.namespace.name }}
{{-   else -}}
{{      default .Release.Namespace .Values.namespace.name }}
{{-   end -}}
{{- end -}}

{{- define "babylon-runner.image" -}}
  {{- if eq .Values.version "main" }}
    {{- printf "%s:latest" .Values.image.repository }}
  {{- else if eq .Values.image.tagOverride "-" }}
    {{- .Values.image.repository }}
  {{- else if .Values.image.tagOverride }}
    {{- printf "%s:%s" .Values.image.repository .Values.image.tagOverride }}
  {{- else }}
    {{- printf "%s:%s" .Values.image.repository .Values.version }}
  {{- end }}
{{- end -}}
