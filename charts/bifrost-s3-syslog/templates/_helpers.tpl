{{- define "b3s.name" -}}{{ .Release.Name }}-bifrost-s3-syslog{{- end -}}
{{- define "b3s.labels" -}}
app.kubernetes.io/name: bifrost-s3-syslog
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
{{- define "b3s.selector" -}}
app.kubernetes.io/name: bifrost-s3-syslog
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
