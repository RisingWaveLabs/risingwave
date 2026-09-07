#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

: "${SOURCE_IMAGE_TAG:?Set SOURCE_IMAGE_TAG to the existing image tag}"
: "${TARGET_IMAGE_TAG:?Set TARGET_IMAGE_TAG to the new image tag}"

DUPLICATE_ON_GHCR="${DUPLICATE_ON_GHCR:-false}"
DUPLICATE_ON_DOCKERHUB="${DUPLICATE_ON_DOCKERHUB:-false}"
ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="docker.io/risingwavelabs/risingwave"

function validateImageTag() {
  local tag="$1"
  local variable_name="$2"

  if [[ ! "$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "Invalid ${variable_name}: ${tag}"
    exit 1
  fi
}

function validateBoolean() {
  local value="$1"
  local variable_name="$2"

  if [[ "${value}" != "true" && "${value}" != "false" ]]; then
    echo "${variable_name} must be true or false"
    exit 1
  fi
}

function ensureImageExists() {
  local image="$1"
  local manifest

  if ! manifest=$(docker buildx imagetools inspect "${image}" 2>&1); then
    echo "Source image does not exist or cannot be inspected: ${image}"
    echo "${manifest}"
    exit 1
  fi

  echo "${manifest}"
}

function ensureImageDoesNotExist() {
  local image="$1"
  local manifest

  if manifest=$(docker buildx imagetools inspect "${image}" 2>&1); then
    echo "Target image already exists: ${image}"
    echo "${manifest}"
    exit 1
  fi

  if ! grep -Eqi "not found|manifest unknown|no such manifest" <<< "${manifest}"; then
    echo "Target image could not be inspected: ${image}"
    echo "${manifest}"
    exit 1
  fi
}

validateImageTag "${SOURCE_IMAGE_TAG}" "SOURCE_IMAGE_TAG"
validateImageTag "${TARGET_IMAGE_TAG}" "TARGET_IMAGE_TAG"
validateBoolean "${DUPLICATE_ON_GHCR}" "DUPLICATE_ON_GHCR"
validateBoolean "${DUPLICATE_ON_DOCKERHUB}" "DUPLICATE_ON_DOCKERHUB"

if [[ "${SOURCE_IMAGE_TAG}" == "${TARGET_IMAGE_TAG}" ]]; then
  echo "SOURCE_IMAGE_TAG and TARGET_IMAGE_TAG must differ"
  exit 1
fi

if [[ "${DUPLICATE_ON_GHCR}" != "true" && "${DUPLICATE_ON_DOCKERHUB}" != "true" ]]; then
  echo "Enable DUPLICATE_ON_GHCR, DUPLICATE_ON_DOCKERHUB, or both"
  exit 1
fi

registries=()
if [[ "${DUPLICATE_ON_GHCR}" == "true" ]]; then
  echo "--- ghcr login"
  echo "${GHCR_TOKEN}" | docker login ghcr.io -u "${GHCR_USERNAME}" --password-stdin
  registries+=("${ghcraddr}")
fi
if [[ "${DUPLICATE_ON_DOCKERHUB}" == "true" ]]; then
  echo "--- dockerhub login"
  echo "${DOCKER_TOKEN}" | docker login docker.io -u "risingwavelabs" --password-stdin
  registries+=("${dockerhubaddr}")
fi

echo "--- check source images exist and target images do not exist"
for registry in "${registries[@]}"; do
  ensureImageExists "${registry}:${SOURCE_IMAGE_TAG}"
  ensureImageDoesNotExist "${registry}:${TARGET_IMAGE_TAG}"
done

echo "--- duplicate image tags"
for registry in "${registries[@]}"; do
  source_image="${registry}:${SOURCE_IMAGE_TAG}"
  target_image="${registry}:${TARGET_IMAGE_TAG}"

  docker buildx imagetools create \
    --tag "${target_image}" \
    "${source_image}"
  docker buildx imagetools inspect "${target_image}"
done
