{{/* Chart fullname, truncated to 63 chars for k8s names. */}}
{{- define "dmo.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dmo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels. */}}
{{- define "dmo.labels" -}}
app.kubernetes.io/name: {{ include "dmo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* Selector labels (stable across upgrades). */}}
{{- define "dmo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dmo.name" . }}
control-plane: controller-manager
{{- end -}}

{{- define "dmo.serviceAccountName" -}}
{{ printf "%s-controller-manager" (include "dmo.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/* Manager Deployment name, 63-char safe. */}}
{{- define "dmo.managerName" -}}
{{ printf "%s-controller-manager" (include "dmo.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/* Metrics Service name, 63-char safe. */}}
{{- define "dmo.metricsServiceName" -}}
{{ printf "%s-metrics" (include "dmo.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Manager RBAC rules — single source of truth, read from files/role.yaml which
`make manifests` keeps in sync with config/rbac/role.yaml (CI diffs the two).
Rendered into the ClusterRole (cluster-wide mode) and into one Role per watched
namespace (namespace-scoped mode) so the two can never diverge.
*/}}
{{- define "dmo.managerRules" -}}
{{- $role := .Files.Get "files/role.yaml" | fromYaml -}}
{{- toYaml $role.rules -}}
{{- end -}}

{{- define "dmo.image" -}}
{{- /* Release images are tagged "v<appVersion>" (release.yml uses the git tag). */ -}}
{{- $tag := .Values.image.tag | default (printf "v%s" .Chart.AppVersion) -}}
{{ .Values.image.repository }}:{{ $tag }}
{{- end -}}
