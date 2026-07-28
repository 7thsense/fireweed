{{/*
Expand the name of the chart.
*/}}
{{- define "fireweed-queue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fireweed-queue.fullname" -}}
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
{{- define "fireweed-queue.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "fireweed-queue.labels" -}}
helm.sh/chart: {{ include "fireweed-queue.chart" . }}
{{ include "fireweed-queue.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "fireweed-queue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fireweed-queue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use.
*/}}
{{- define "fireweed-queue.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fireweed-queue.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Select the image tag.
*/}}
{{- define "fireweed-queue.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{/*
Name of the storage persistent volume claim.
*/}}
{{- define "fireweed-queue.storagePvcName" -}}
{{- default (printf "%s-storage" (include "fireweed-queue.fullname" .)) .Values.persistence.existingClaim -}}
{{- end -}}

{{/*
True when the selected log axis is a local/NAS filesystem object log
(first-class filesystem, or legacy objectlog + store=local).
*/}}
{{- define "fireweed-queue.logIsFilesystemLocal" -}}
{{- or (eq .Values.storage.log.backend "filesystem") (and (eq .Values.storage.log.backend "objectlog") (eq .Values.storage.log.objectLog.store "local")) -}}
{{- end -}}

{{/*
True when the selected log axis is S3-compatible object storage
(first-class s3, or legacy objectlog + store=s3).
*/}}
{{- define "fireweed-queue.logIsS3" -}}
{{- or (eq .Values.storage.log.backend "s3") (and (eq .Values.storage.log.backend "objectlog") (eq .Values.storage.log.objectLog.store "s3")) -}}
{{- end -}}

{{/*
True when the selected projection needs a local durable image path under the storage volume.
*/}}
{{- define "fireweed-queue.projectionNeedsLocalVolume" -}}
{{- eq .Values.storage.projection.backend "sqlite" -}}
{{- end -}}

{{/*
True when the pod needs a local data volume (filesystem/sqlite log or durable local projection).
*/}}
{{- define "fireweed-queue.needsLocalVolume" -}}
{{- or (eq (include "fireweed-queue.logIsFilesystemLocal" .) "true") (eq .Values.storage.log.backend "sqlite") (eq (include "fireweed-queue.projectionNeedsLocalVolume" .) "true") -}}
{{- end -}}

{{/*
Fail closed when a multi-replica deployment is not using the replica-safe shared
S3/Postgres profile. Local filesystem object-log storage stays single-replica only.
Shared profile: log=s3 (or objectlog+store=s3) × controlPlane=postgres × projection=sqlite.
*/}}
{{- define "fireweed-queue.validateReplicaProfile" -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $s3Log := eq (include "fireweed-queue.logIsS3" .) "true" -}}
{{- $shared := and $s3Log (eq .Values.storage.controlPlane.backend "postgres") (eq .Values.storage.projection.backend "sqlite") -}}
{{- if gt $replicas 1 -}}
{{- if not $shared -}}
{{- fail "replicaCount > 1 requires storage.log.backend=s3 (or objectlog with objectLog.store=s3), storage.controlPlane.backend=postgres, storage.projection.backend=sqlite, and persistence.enabled=false" -}}
{{- end -}}
{{- if .Values.persistence.enabled -}}
{{- fail "replicaCount > 1 requires persistence.enabled=false so SQLite projections stay pod-local" -}}
{{- end -}}
{{- end -}}
{{- end -}}
