{{/*
Expand the name of the chart.
*/}}
{{- define "pqueue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pqueue.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "pqueue.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "pqueue.labels" -}}
helm.sh/chart: {{ include "pqueue.chart" . }}
{{ include "pqueue.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "pqueue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pqueue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use.
*/}}
{{- define "pqueue.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pqueue.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Select the image tag.
*/}}
{{- define "pqueue.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{/*
Name of the storage persistent volume claim.
*/}}
{{- define "pqueue.storagePvcName" -}}
{{- default (printf "%s-storage" (include "pqueue.fullname" .)) .Values.persistence.existingClaim -}}
{{- end -}}

{{/*
Fail closed when a multi-replica deployment is not using the replica-safe shared
S3/Postgres profile. Local object-log storage stays single-replica only.
*/}}
{{- define "pqueue.validateReplicaProfile" -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $shared := and (eq .Values.storage.log.backend "objectlog") (eq .Values.storage.log.objectLog.store "s3") (eq .Values.storage.controlPlane.backend "postgres") (eq .Values.storage.projection.backend "sqlite") -}}
{{- if gt $replicas 1 -}}
{{- if not $shared -}}
{{- fail "replicaCount > 1 requires storage.log.backend=objectlog, storage.log.objectLog.store=s3, storage.controlPlane.backend=postgres, storage.projection.backend=sqlite, and persistence.enabled=false" -}}
{{- end -}}
{{- if .Values.persistence.enabled -}}
{{- fail "replicaCount > 1 requires persistence.enabled=false so SQLite projections stay pod-local" -}}
{{- end -}}
{{- end -}}
{{- end -}}
