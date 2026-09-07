# Docker sccache cache

This Terraform configuration sketches the AWS resources needed for an
S3-backed `sccache` cache used by ad-hoc Docker image builds.

The cache is intended to live in a shared build-cache bucket and use a scoped
prefix, instead of creating a new bucket for each cache type. By default, Docker
compiler objects are stored under `sccache/docker/`.

It intentionally does not create or store any long-lived access keys. The
recommended path is:

1. Create or select one shared build-cache bucket.
2. Create the scoped IAM role with this configuration.
3. Set `DOCKER_SCCACHE_BUCKET`, `DOCKER_SCCACHE_REGION`, and
   `DOCKER_SCCACHE_ROLE_ARN` in the Buildkite docker pipeline environment.
   Optionally set `DOCKER_SCCACHE_PREFIX_ROOT` if you changed the default
   prefix.
4. Trigger a Docker image build with `ENABLE_DOCKER_SCCACHE=true`.

After initializing the shared backend as described below, an example that
creates the shared build-cache bucket is:

```bash
terraform plan \
  -var='bucket_name=<shared-build-cache-bucket>' \
  -var='buildkite_agent_role_arns=["arn:aws:iam::<account-id>:role/<buildkite-agent-role>"]'
```

Example that reuses an existing shared build-cache bucket:

```bash
terraform plan \
  -var='create_bucket=false' \
  -var='bucket_name=<shared-build-cache-bucket>' \
  -var='manage_lifecycle_rules=false' \
  -var='buildkite_agent_role_arns=["arn:aws:iam::<account-id>:role/<buildkite-agent-role>"]'
```

`manage_lifecycle_rules=false` is recommended when the existing bucket's
lifecycle configuration is owned by another Terraform module, because the AWS
provider manages the whole bucket lifecycle configuration as one resource.
When unset, `manage_lifecycle_rules` defaults to `create_bucket`, so existing
buckets are not modified unless this is explicitly enabled.

The provider defaults to the `rwc-cicd` AWS profile.

## Shared Terraform state

This stack uses an S3 backend with native lockfiles. The backend bucket must be
a pre-existing, versioned Terraform state bucket managed outside this stack; do
not use a bucket that this configuration is expected to create. Terraform 1.10
or later is required for S3 lockfiles.

The state-bucket IAM principal needs `s3:ListBucket` on the bucket,
`s3:GetObject` and `s3:PutObject` on
`risingwave/docker-sccache/terraform.tfstate`, and `s3:GetObject`,
`s3:PutObject`, and `s3:DeleteObject` on the adjacent `.tflock` object.

Supply the deployment-specific bucket and region during initialization. Do not
put credentials in backend configuration:

```bash
AWS_PROFILE=rwc-cicd terraform init \
  -backend-config='bucket=<terraform-state-bucket>' \
  -backend-config='region=<terraform-state-bucket-region>'
```

### Migrate the existing local state

The maintainer who has the state from the original apply must first back up
`terraform.tfstate`, then migrate it into the shared backend:

```bash
cp terraform.tfstate terraform.tfstate.pre-s3-backend
AWS_PROFILE=rwc-cicd terraform init -migrate-state \
  -backend-config='bucket=<terraform-state-bucket>' \
  -backend-config='region=<terraform-state-bucket-region>'
```

After migration, run `terraform plan` and verify that it proposes no resource
creation or replacement before applying further changes.

### Recover when the original state is unavailable

If the resources were applied but the original local state cannot be recovered,
initialize the empty shared backend, set the same `TF_VAR_*` values used by the
deployment, and import every resource that this stack owns before planning:

```bash
AWS_PROFILE=rwc-cicd terraform init -reconfigure \
  -backend-config='bucket=<terraform-state-bucket>' \
  -backend-config='region=<terraform-state-bucket-region>'
export TF_VAR_bucket_name=<cache-bucket-name>
```

Depending on `create_bucket`, `manage_lifecycle_rules`, and whether an
assumable role was created, the imports are:

```bash
terraform import 'aws_s3_bucket.docker_sccache[0]' <cache-bucket-name>
terraform import 'aws_s3_bucket_public_access_block.docker_sccache[0]' <cache-bucket-name>
terraform import 'aws_s3_bucket_ownership_controls.docker_sccache[0]' <cache-bucket-name>
terraform import 'aws_s3_bucket_server_side_encryption_configuration.docker_sccache[0]' <cache-bucket-name>
terraform import 'aws_s3_bucket_lifecycle_configuration.docker_sccache[0]' <cache-bucket-name>
terraform import aws_iam_policy.docker_sccache <policy-arn>
terraform import 'aws_iam_role.docker_sccache[0]' <role-name>
terraform import 'aws_iam_role_policy_attachment.docker_sccache[0]' '<role-name>/<policy-arn>'
```

Skip addresses disabled by the selected variables. Do not run `terraform apply`
until all existing owned resources are imported and `terraform plan` has been
checked for unintended creates, replacements, or deletes.
