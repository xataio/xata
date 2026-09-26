{{- define "projects.setupContainer" -}}
- name: {{ .Chart.Name }}-init
  image: "{{ .Values.image.repository }}{{ if .Values.image.digest }}@{{ .Values.image.digest }}{{ else }}:{{ .Values.image.tag }}{{ end }}"
  command: ["/server"]
  args: ["setup"]
  env:
    - name: AUTH_GRPC_URL
      value: {{ .Values.dependencies.authGrpcUrl | quote }}
    - name: CLUSTERS_GRPC_URL
      value: {{ .Values.dependencies.clustersGrpcUrl | quote }}
    - name: POSTGRES_SSLMODE
      value: {{ .Values.postgres.sslmode | quote }}
    - name: POSTGRES_DB
      value: {{ .Values.postgres.database | quote }}
    {{- if .Values.postgres.existingSecret }}
    - name: POSTGRES_USER
      valueFrom:
        secretKeyRef:
          name: {{ .Values.postgres.existingSecret }}
          key: {{ .Values.postgres.userKey }}
    - name: POSTGRES_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ .Values.postgres.existingSecret }}
          key: {{ .Values.postgres.passwordKey }}
    {{- end }}
    {{- if .Values.metastoreReader.enabled }}
    - name: METASTORE_READER_USER
      valueFrom:
        secretKeyRef:
          name: {{ .Values.metastoreReader.existingSecret }}
          key: {{ .Values.metastoreReader.userKey }}
    - name: METASTORE_READER_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ .Values.metastoreReader.existingSecret }}
          key: {{ .Values.metastoreReader.passwordKey }}
    {{- end }}
    - name: POSTGRES_HOST
      value: {{ .Values.postgres.host | quote }}
    {{- if .Values.postgres.port }}
    - name: POSTGRES_PORT
      value: {{ .Values.postgres.port | quote }}
    {{- end }}
    {{- if .Values.gateway.hostport }}
    - name: GATEWAY_HOSTPORT
      value: {{ .Values.gateway.hostport | quote }}
    {{- end }}
    {{- if .Values.defaultRegion }}
    - name: DEFAULT_REGION
      value: {{ .Values.defaultRegion | quote }}
    {{- end }}
  {{- if .Values.xataSecrets.enabled }}
  envFrom:
    - secretRef:
        name: {{ .Values.xataSecrets.secretName }}
  {{- end }}
  resources:
    {{- toYaml .Values.resources | nindent 4 }}
  volumeMounts:
    - name: scheduler-config
      mountPath: /config
      readOnly: true
  securityContext:
    runAsUser: 1000
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    runAsNonRoot: true
    capabilities:
      drop:
        - ALL
{{- end -}}
