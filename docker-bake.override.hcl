variable "DESTINATIONS" {
  type    = list(string)
  default = ["ghcr.io/xataio/xata"]
}

variable "SOURCE_URL" {
  default = "https://github.com/xataio/xata"
}

target "keycloak" {
  args = {
    NO_THEME = "true"
  }
}

group "default" {
  targets = [
    "auth",
    "projects",
    "clusters",
    "gateway",
    "branch-operator",
    "branching-replica-operator",
    "scale-to-zero-sidecar",
    "keycloak",
  ]
}
