locals {
  docker_sccache_prefix = "${trimsuffix(var.docker_sccache_prefix, "/")}/"
  manage_lifecycle      = coalesce(var.manage_lifecycle_rules, var.create_bucket)
  bucket_id             = var.create_bucket ? aws_s3_bucket.docker_sccache[0].id : data.aws_s3_bucket.docker_sccache[0].id
  bucket_arn            = var.create_bucket ? aws_s3_bucket.docker_sccache[0].arn : data.aws_s3_bucket.docker_sccache[0].arn
  bucket_name           = var.create_bucket ? aws_s3_bucket.docker_sccache[0].bucket : data.aws_s3_bucket.docker_sccache[0].bucket
}

resource "aws_s3_bucket" "docker_sccache" {
  count = var.create_bucket ? 1 : 0

  bucket = var.bucket_name

  tags = var.tags
}

data "aws_s3_bucket" "docker_sccache" {
  count = var.create_bucket ? 0 : 1

  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "docker_sccache" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.docker_sccache[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "docker_sccache" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.docker_sccache[0].id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "docker_sccache" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.docker_sccache[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "docker_sccache" {
  count = local.manage_lifecycle ? 1 : 0

  bucket = local.bucket_id

  rule {
    id     = "expire-docker-sccache"
    status = "Enabled"

    filter {
      prefix = local.docker_sccache_prefix
    }

    expiration {
      days = var.cache_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

data "aws_iam_policy_document" "docker_sccache" {
  statement {
    sid = "ListDockerSccachePrefix"

    actions = [
      "s3:ListBucket",
    ]

    resources = [
      local.bucket_arn,
    ]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${local.docker_sccache_prefix}*",
      ]
    }
  }

  statement {
    sid = "ReadWriteDockerSccacheObjects"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]

    resources = [
      "${local.bucket_arn}/${local.docker_sccache_prefix}*",
    ]
  }
}

resource "aws_iam_policy" "docker_sccache" {
  name_prefix = "docker-sccache-"
  description = "Read/write access to the Docker sccache S3 prefix."
  policy      = data.aws_iam_policy_document.docker_sccache.json
  tags        = var.tags
}

data "aws_iam_policy_document" "assume_by_buildkite" {
  count = length(var.buildkite_agent_role_arns) > 0 ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = var.buildkite_agent_role_arns
    }
  }
}

resource "aws_iam_role" "docker_sccache" {
  count = length(var.buildkite_agent_role_arns) > 0 ? 1 : 0

  name_prefix          = "docker-sccache-"
  assume_role_policy   = data.aws_iam_policy_document.assume_by_buildkite[0].json
  max_session_duration = var.max_session_duration_seconds
  tags                 = var.tags
}

resource "aws_iam_role_policy_attachment" "docker_sccache" {
  count = length(var.buildkite_agent_role_arns) > 0 ? 1 : 0

  role       = aws_iam_role.docker_sccache[0].name
  policy_arn = aws_iam_policy.docker_sccache.arn
}
