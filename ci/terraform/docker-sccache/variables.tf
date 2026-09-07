variable "aws_profile" {
  description = "AWS profile used for planning and applying these resources."
  type        = string
  default     = "rwc-cicd"
}

variable "aws_region" {
  description = "AWS region for the shared build cache bucket."
  type        = string
  default     = "us-east-2"
}

variable "bucket_name" {
  description = "Stable shared build cache bucket name. The module creates it when create_bucket is true, or reuses it when create_bucket is false."
  type        = string
}

variable "create_bucket" {
  description = "Whether to create the shared build cache bucket. Set false to use an existing bucket."
  type        = bool
  default     = true
}

variable "docker_sccache_prefix" {
  description = "S3 key prefix reserved for Docker sccache objects inside the shared build cache bucket."
  type        = string
  default     = "sccache/docker/"
}

variable "cache_retention_days" {
  description = "How long to retain cached compiler objects under the Docker sccache prefix."
  type        = number
  default     = 60
}

variable "manage_lifecycle_rules" {
  description = "Whether this module should manage lifecycle rules for the Docker sccache prefix. Defaults to create_bucket when null."
  type        = bool
  default     = null
}

variable "buildkite_agent_role_arns" {
  description = "IAM role ARNs used by Buildkite agents that may assume the Docker sccache role."
  type        = list(string)
  default     = []
}

variable "max_session_duration_seconds" {
  description = "Maximum STS session duration for the Docker sccache role."
  type        = number
  default     = 14400
}

variable "tags" {
  description = "Tags to apply to created resources."
  type        = map(string)
  default = {
    Service = "buildkite"
    Purpose = "build-cache"
  }
}
