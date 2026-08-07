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
True when the selected log axis is a local/NAS filesystem object log.
*/}}
{{- define "fireweed-queue.logIsFilesystemLocal" -}}
{{- eq .Values.storage.log.backend "filesystem" -}}
{{- end -}}

{{/*
True when the selected log axis is S3-compatible object storage.
*/}}
{{- define "fireweed-queue.logIsS3" -}}
{{- eq .Values.storage.log.backend "s3" -}}
{{- end -}}

{{/*
True when the selected projection needs a local durable image path under the storage volume.
sqlite and turso are pod-local durable projections (rebuildable from a durable log).
*/}}
{{- define "fireweed-queue.projectionNeedsLocalVolume" -}}
{{- or (eq .Values.storage.projection.backend "sqlite") (eq .Values.storage.projection.backend "turso") -}}
{{- end -}}

{{/*
True when the selected projection is a pod-local rebuildable image (not shared postgres/memory).
Used by multi-replica durability rules: shared S3 log + postgres control plane + local projection.
*/}}
{{- define "fireweed-queue.projectionIsPodLocalRebuildable" -}}
{{- or (eq .Values.storage.projection.backend "sqlite") (eq .Values.storage.projection.backend "turso") -}}
{{- end -}}

{{/*
True when the pod needs a local data volume (filesystem/sqlite log or durable local projection).
*/}}
{{- define "fireweed-queue.needsLocalVolume" -}}
{{- or (eq (include "fireweed-queue.logIsFilesystemLocal" .) "true") (eq .Values.storage.log.backend "sqlite") (eq (include "fireweed-queue.projectionNeedsLocalVolume" .) "true") -}}
{{- end -}}

{{/*
Fail closed when a multi-replica deployment is not using a replica-safe shared
profile. Local filesystem object-log storage stays single-replica only.

Durability/control-plane rules (not a hard-coded SQLite projection):
  log=s3 (shared durable command log)
  controlPlane=postgres (ownership)
  projection is pod-local rebuildable (sqlite|turso)
  persistence.enabled=false (emptyDir so each pod keeps a private projection image)
*/}}
{{- define "fireweed-queue.validateReplicaProfile" -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $s3Log := eq (include "fireweed-queue.logIsS3" .) "true" -}}
{{- $localProj := eq (include "fireweed-queue.projectionIsPodLocalRebuildable" .) "true" -}}
{{- $shared := and $s3Log (eq .Values.storage.controlPlane.backend "postgres") $localProj -}}
{{- if gt $replicas 1 -}}
{{- if not $shared -}}
{{- fail "replicaCount > 1 requires storage.log.backend=s3, storage.controlPlane.backend=postgres, a pod-local rebuildable projection (sqlite|turso), and persistence.enabled=false" -}}
{{- end -}}
{{- if .Values.persistence.enabled -}}
{{- fail "replicaCount > 1 requires persistence.enabled=false so pod-local projections (sqlite|turso) stay private per pod" -}}
{{- end -}}
{{- end -}}
{{- end -}}
