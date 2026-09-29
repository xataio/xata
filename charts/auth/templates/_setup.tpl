{{- define "auth.setupContainer" -}}
- name: {{ .Chart.Name }}-init
  image: "{{ .Values.image.repository }}{{ if .Values.image.digest }}@{{ .Values.image.digest }}{{ else }}:{{ .Values.image.tag }}{{ end }}"
  command: ["/server"]
  args: ["setup"]
  env:
    - name: AUTH_GRPC_URL
      value: "{{ .Values.dependencies.authGrpcUrl }}"
    - name: PROJECTS_GRPC_URL
      value: "{{ .Values.dependencies.projectsGrpcUrl }}"
    - name: POSTGRES_SSLMODE
      value: {{ .Values.postgres.sslmode }}
    - name: API_KEY_HMAC_SECRET
      valueFrom:
        secretKeyRef:
          name: auth-api-key-hmac
          key: hmac-secret
    {{- with .Values.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  envFrom:
    {{- with .Values.extraEnvFrom }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
    - secretRef:
        name: auth-api-key-hmac
  resources:
    {{- toYaml .Values.resources | nindent 4 }}
  securityContext:
    runAsUser: 1000
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    runAsNonRoot: true
    capabilities:
      drop:
        - ALL
{{- end -}}
