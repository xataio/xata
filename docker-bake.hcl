variable "TAG" {
  default = "latest"
}

variable "LATEST" {
  default = false
}

function "image_tags" {
  params = [image]
  result = concat(
    [for destination in DESTINATIONS : "${destination}/${image}:${TAG}"],
    LATEST ? [for destination in DESTINATIONS : "${destination}/${image}:latest"] : [],
  )
}

function "cache_ref" {
  params = [image]
  result = "${DESTINATIONS[0]}/${image}:buildcache"
}

target "_service" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]

  args = {
    # Fixed Y2K epoch: image timestamps must not vary between builds.
    SOURCE_DATE_EPOCH = "946684800"
  }

  output = ["type=image,rewrite-timestamp=true"]

  labels = {
    "org.opencontainers.image.source" = SOURCE_URL
  }
}

target "auth" {
  inherits = ["_service"]
  args = {
    SERVICE_NAME = "auth"
  }
  tags       = image_tags("auth")
  cache-from = ["type=registry,ref=${cache_ref("auth")}"]
  cache-to   = ["type=registry,ref=${cache_ref("auth")},mode=max"]
}

target "projects" {
  inherits = ["_service"]
  args = {
    SERVICE_NAME = "projects"
  }
  tags       = image_tags("projects")
  cache-from = ["type=registry,ref=${cache_ref("projects")}"]
  cache-to   = ["type=registry,ref=${cache_ref("projects")},mode=max"]
}

target "clusters" {
  inherits = ["_service"]
  args = {
    SERVICE_NAME = "clusters"
  }
  tags       = image_tags("clusters")
  cache-from = ["type=registry,ref=${cache_ref("clusters")}"]
  cache-to   = ["type=registry,ref=${cache_ref("clusters")},mode=max"]
}

target "gateway" {
  inherits = ["_service"]
  args = {
    SERVICE_NAME = "gateway"
  }
  tags       = image_tags("gateway")
  cache-from = ["type=registry,ref=${cache_ref("gateway")}"]
  cache-to   = ["type=registry,ref=${cache_ref("gateway")},mode=max"]
}

target "branch-operator" {
  inherits = ["_service"]
  args = {
    SERVICE_NAME = "branch-operator"
  }
  tags       = image_tags("branch-operator")
  cache-from = ["type=registry,ref=${cache_ref("branch-operator")}"]
  cache-to   = ["type=registry,ref=${cache_ref("branch-operator")},mode=max"]
}

target "branching-replica-operator" {
  inherits = ["_service"]
  args = {
    SERVICE_NAME = "branching-replica-operator"
  }
  tags       = image_tags("branching-replica-operator")
  cache-from = ["type=registry,ref=${cache_ref("branching-replica-operator")}"]
  cache-to   = ["type=registry,ref=${cache_ref("branching-replica-operator")},mode=max"]
}

target "scale-to-zero-sidecar" {
  inherits   = ["_service"]
  dockerfile = "services/scale-to-zero-sidecar/Dockerfile"
  tags       = image_tags("scale-to-zero-sidecar")
  cache-from = ["type=registry,ref=${cache_ref("scale-to-zero-sidecar")}"]
  cache-to   = ["type=registry,ref=${cache_ref("scale-to-zero-sidecar")},mode=max"]
}

target "keycloak" {
  context    = "dev/docker/keycloak"
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]

  args = {
    # Fixed Y2K epoch: image timestamps must not vary between builds.
    SOURCE_DATE_EPOCH = "946684800"
  }

  output = ["type=image,rewrite-timestamp=true"]

  labels = {
    "org.opencontainers.image.source" = SOURCE_URL
  }
  secret     = ["id=GIT_TOKEN,env=GIT_TOKEN"]
  tags       = image_tags("keycloak")
  cache-from = ["type=registry,ref=${cache_ref("keycloak")}"]
  cache-to   = ["type=registry,ref=${cache_ref("keycloak")},mode=max"]
}
