moved {
  from = aws_s3_bucket.docker_sccache
  to   = aws_s3_bucket.docker_sccache[0]
}

moved {
  from = aws_s3_bucket_public_access_block.docker_sccache
  to   = aws_s3_bucket_public_access_block.docker_sccache[0]
}

moved {
  from = aws_s3_bucket_ownership_controls.docker_sccache
  to   = aws_s3_bucket_ownership_controls.docker_sccache[0]
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.docker_sccache
  to   = aws_s3_bucket_server_side_encryption_configuration.docker_sccache[0]
}

moved {
  from = aws_s3_bucket_lifecycle_configuration.docker_sccache
  to   = aws_s3_bucket_lifecycle_configuration.docker_sccache[0]
}
