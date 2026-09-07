#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"
CARGO_PROFILE=${CARGO_PROFILE:-production}
ENABLE_DOCKER_SCCACHE=${ENABLE_DOCKER_SCCACHE:-false}
DOCKER_SCCACHE_REGION=${DOCKER_SCCACHE_REGION:-us-east-2}
PUSH_DOCKERHUB=${PUSH_DOCKERHUB:-true}

sanitize_cache_component() {
  local value="$1"
  value="${value//\//-}"
  value="$(printf "%s" "$value" | tr -c 'A-Za-z0-9._=-' '-')"
  value="${value##-}"
  value="${value%%-}"
  if [[ -z "${value}" ]]; then
    value="unknown"
  fi
  printf "%s" "$value"
}

build_secret_args=()
docker_build_args=(
  --build-arg "GIT_SHA=${BUILDKITE_COMMIT}"
  --build-arg "CARGO_PROFILE=${CARGO_PROFILE}"
  --build-arg "ENABLE_DOCKER_SCCACHE=${ENABLE_DOCKER_SCCACHE}"
)

cleanup_docker_sccache_credentials() {
  if [[ -n "${docker_sccache_credentials_dir:-}" ]]; then
    rm -rf "${docker_sccache_credentials_dir}"
  fi
}
trap cleanup_docker_sccache_credentials EXIT

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

if [[ "${PUSH_DOCKERHUB}" == "true" ]]; then
  echo "--- dockerhub login"
  echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin
fi

if [[ -n "${ORIGINAL_IMAGE_TAG+x}" ]] && [[ -n "${NEW_IMAGE_TAG+x}" ]]; then
  echo "--- retag docker image"
  docker pull ${ghcraddr}:${ORIGINAL_IMAGE_TAG}
  docker tag ${ghcraddr}:${ORIGINAL_IMAGE_TAG} ${ghcraddr}:${NEW_IMAGE_TAG}-${arch}
  docker push ${ghcraddr}:${NEW_IMAGE_TAG}-${arch}
  exit 0
fi

# Build RisingWave docker image ${BUILDKITE_COMMIT}-${arch}
echo "--- docker build and tag"
echo "CARGO_PROFILE is set to ${CARGO_PROFILE}"
docker buildx create \
  --name container \
  --driver=docker-container

pull_param=()
if [[ "${ALWAYS_PULL:-false}" = "true" ]]; then
  pull_param=(--pull)
fi

if [[ "${ENABLE_DOCKER_SCCACHE}" == "true" ]]; then
  echo "--- configure docker sccache"
  if [[ -z "${DOCKER_SCCACHE_BUCKET:-}" ]]; then
    echo "DOCKER_SCCACHE_BUCKET must be set when ENABLE_DOCKER_SCCACHE=true" >&2
    exit 1
  fi

  if [[ -n "${DOCKER_SCCACHE_ROLE_ARN:-}" ]]; then
    if ! command -v aws >/dev/null 2>&1; then
      echo "aws CLI is required to assume DOCKER_SCCACHE_ROLE_ARN" >&2
      exit 1
    fi

    read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN < <(
      aws sts assume-role \
        --role-arn "${DOCKER_SCCACHE_ROLE_ARN}" \
        --role-session-name "${DOCKER_SCCACHE_ROLE_SESSION_NAME:-docker-sccache-${BUILDKITE_BUILD_NUMBER:-local}}" \
        --duration-seconds "${DOCKER_SCCACHE_SESSION_DURATION_SECONDS:-14400}" \
        --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
        --output text
    )
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  fi

  if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    echo "AWS credentials are required for docker sccache. Set DOCKER_SCCACHE_ROLE_ARN or provide AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY in the environment." >&2
    exit 1
  fi

  docker_sccache_credentials_dir="$(mktemp -d)"
  {
    echo "[default]"
    printf "aws_access_key_id = %s\n" "${AWS_ACCESS_KEY_ID}"
    printf "aws_secret_access_key = %s\n" "${AWS_SECRET_ACCESS_KEY}"
    if [[ -n "${AWS_SESSION_TOKEN:-}" ]]; then
      printf "aws_session_token = %s\n" "${AWS_SESSION_TOKEN}"
    fi
  } >"${docker_sccache_credentials_dir}/credentials"
  chmod 0600 "${docker_sccache_credentials_dir}/credentials"

  {
    echo "[default]"
    printf "region = %s\n" "${DOCKER_SCCACHE_REGION}"
  } >"${docker_sccache_credentials_dir}/config"

  cache_branch="$(sanitize_cache_component "${BUILDKITE_BRANCH:-unknown}")"
  DOCKER_SCCACHE_PREFIX_ROOT="${DOCKER_SCCACHE_PREFIX_ROOT:-sccache/docker}"
  DOCKER_SCCACHE_PREFIX_ROOT="${DOCKER_SCCACHE_PREFIX_ROOT#/}"
  DOCKER_SCCACHE_PREFIX_ROOT="${DOCKER_SCCACHE_PREFIX_ROOT%/}"
  DOCKER_SCCACHE_PREFIX=${DOCKER_SCCACHE_PREFIX:-"${DOCKER_SCCACHE_PREFIX_ROOT}/${cache_branch}/${arch}/${CARGO_PROFILE}"}
  echo "Docker sccache prefix: ${DOCKER_SCCACHE_PREFIX}"

  build_secret_args+=(
    --secret "id=aws_credentials,src=${docker_sccache_credentials_dir}/credentials"
    --secret "id=aws_config,src=${docker_sccache_credentials_dir}/config"
  )
  docker_build_args+=(
    --build-arg "DOCKER_SCCACHE_BUCKET=${DOCKER_SCCACHE_BUCKET}"
    --build-arg "DOCKER_SCCACHE_REGION=${DOCKER_SCCACHE_REGION}"
    --build-arg "DOCKER_SCCACHE_PREFIX=${DOCKER_SCCACHE_PREFIX}"
  )
fi

docker buildx build -f docker/Dockerfile \
  "${docker_build_args[@]}" \
  -t "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" \
  --progress plain \
  --builder=container \
  --load \
  "${pull_param[@]}" \
  "${build_secret_args[@]}" \
  --cache-to "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:${arch}" \
  --cache-from "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:${arch}" \
  .

echo "--- check the image can start correctly"
container_id=$(docker run -d "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" playground)
sleep 10
container_status=$(docker inspect --format='{{.State.Status}}' "$container_id")
if [ "$container_status" != "running" ]; then
  echo "docker run failed with status $container_status"
  docker inspect "$container_id"
  docker logs "$container_id"
  exit 1
fi

echo "--- docker images"
docker images

echo "--- docker push to ghcr"
docker push "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- docker push to dockerhub"
if [[ "${PUSH_DOCKERHUB}" == "true" ]]; then
  docker tag "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"
  docker push "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"
else
  echo "Skipped because PUSH_DOCKERHUB=${PUSH_DOCKERHUB}"
fi
