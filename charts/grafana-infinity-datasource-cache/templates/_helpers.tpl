{{- define "gidc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "gidc.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "gidc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "gidc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gidc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "gidc.labels" -}}
helm.sh/chart: {{ include "gidc.chart" . }}
{{ include "gidc.selectorLabels" . }}
app.kubernetes.io/version: {{ include "gidc.version" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "gidc.version" -}}
{{- .Values.image.tag | default .Chart.AppVersion | trimPrefix "v" }}
{{- end }}

{{- define "gidc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gidc.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* The service refuses to start without sources; fail at render time instead of in a crash loop. */}}
{{- define "gidc.validate" -}}
{{- $envSources := false }}
{{- range .Values.extraEnv }}
{{- if eq .name "SOURCES" }}{{ $envSources = true }}{{ end }}
{{- end }}
{{- if and (not .Values.config.sources) (not $envSources) }}
{{- fail "config.sources: define at least one source (see values.yaml and config.example.yml)" }}
{{- end }}
{{- end }}
