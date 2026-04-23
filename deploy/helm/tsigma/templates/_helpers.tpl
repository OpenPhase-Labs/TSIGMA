{{/*
Expand the name of the chart.
*/}}
{{- define "tsigma.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "tsigma.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Chart name and version label.
*/}}
{{- define "tsigma.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "tsigma.labels" -}}
helm.sh/chart: {{ include "tsigma.chart" . }}
{{ include "tsigma.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "tsigma.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tsigma.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
ServiceAccount name.
*/}}
{{- define "tsigma.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tsigma.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Secret name for the database password (existing or chart-managed).
*/}}
{{- define "tsigma.dbSecretName" -}}
{{- if .Values.database.existingSecret -}}
{{- .Values.database.existingSecret -}}
{{- else -}}
{{- printf "%s-db" (include "tsigma.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Secret name for the admin password / OIDC / OAuth2 secrets.
*/}}
{{- define "tsigma.authSecretName" -}}
{{- printf "%s-auth" (include "tsigma.fullname" .) -}}
{{- end -}}

{{/*
Image reference (repo:tag) — falls back to .Chart.AppVersion when image.tag is empty.
*/}}
{{- define "tsigma.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
