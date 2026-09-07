#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

: "${IMAGE_TAG:?Set IMAGE_TAG to the image tag to sync}"

GHCR_TO_DOCKERHUB="${GHCR_TO_DOCKERHUB:-true}"
ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="docker.io/risingwavelabs/risingwave"

function validateImageTag() {
  local tag="$1"

  if [[ ! "$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "Invalid IMAGE_TAG: ${tag}"
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

validateImageTag "${IMAGE_TAG}"

case "${GHCR_TO_DOCKERHUB}" in
  true)
    source_image="${ghcraddr}:${IMAGE_TAG}"
    target_image="${dockerhubaddr}:${IMAGE_TAG}"
    ;;
  false)
    source_image="${dockerhubaddr}:${IMAGE_TAG}"
    target_image="${ghcraddr}:${IMAGE_TAG}"
    ;;
  *)
    echo "GHCR_TO_DOCKERHUB must be true or false"
    exit 1
    ;;
esac

echo "--- ghcr login"
echo "${GHCR_TOKEN}" | docker login ghcr.io -u "${GHCR_USERNAME}" --password-stdin

echo "--- dockerhub login"
echo "${DOCKER_TOKEN}" | docker login docker.io -u "risingwavelabs" --password-stdin

echo "--- check source image exists"
ensureImageExists "${source_image}"

echo "--- check target image does not exist"
ensureImageDoesNotExist "${target_image}"

echo "--- sync image"
docker buildx imagetools create \
  --tag "${target_image}" \
  "${source_image}"

echo "--- inspect synced image"
docker buildx imagetools inspect "${target_image}"
