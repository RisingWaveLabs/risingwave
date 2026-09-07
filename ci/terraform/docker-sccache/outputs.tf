output "bucket_name" {
  description = "Shared build cache S3 bucket name."
  value       = local.bucket_name
}

output "bucket_region" {
  description = "S3 bucket region."
  value       = var.aws_region
}

output "iam_policy_arn" {
  description = "IAM policy ARN with scoped Docker sccache S3 permissions."
  value       = aws_iam_policy.docker_sccache.arn
}

output "docker_sccache_prefix" {
  description = "S3 key prefix reserved for Docker sccache objects."
  value       = local.docker_sccache_prefix
}

output "role_arn" {
  description = "Role ARN for Buildkite agents to assume. Null when no agent roles were provided."
  value       = try(aws_iam_role.docker_sccache[0].arn, null)
}

output "buildkite_environment" {
  description = "Non-secret Buildkite environment values to set when enabling Docker sccache."
  value = {
    DOCKER_SCCACHE_BUCKET      = local.bucket_name
    DOCKER_SCCACHE_REGION      = var.aws_region
    DOCKER_SCCACHE_PREFIX_ROOT = trimsuffix(local.docker_sccache_prefix, "/")
    DOCKER_SCCACHE_ROLE_ARN    = try(aws_iam_role.docker_sccache[0].arn, null)
  }
}
