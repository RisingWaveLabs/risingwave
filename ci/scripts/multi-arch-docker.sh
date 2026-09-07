#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Create multi-arch docker images from ${BUILDKITE_COMMIT}-x86_64 and ${BUILDKITE_COMMIT}-aarch64
# They are created by ci/scripts/docker.sh
#
# Also add addtional tags to the images:
# nightly-yyyyMMdd: nightly build in main-cron
# vX.Y.Z-alpha.yyyyMMdd: nightly build in main-cron, semver tag for compatibility
# latest: the latest stable build
# nightly: the latest nightly build

export PATH=$PATH:/var/lib/buildkite-agent/.local/bin

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
PUSH_DOCKERHUB=${PUSH_DOCKERHUB:-true}


arches=()

if [ "${SKIP_TARGET_AMD64:-false}" != "true" ]; then
  arches+=("x86_64")
fi

if [ "${SKIP_TARGET_AARCH64:-false}" != "true" ]; then
  arches+=("aarch64")
fi

echo "--- arches: ${arches[*]}"

# push images to gchr
function pushGchr() {
  GHCRTAG="${ghcraddr}:$1"
  echo "push to gchr, image tag: ${GHCRTAG}"
  args=()
  for arch in "${arches[@]}"
  do
    args+=( --amend "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" )
  done
  docker manifest create --insecure "$GHCRTAG" "${args[@]}"
  docker manifest push --insecure "$GHCRTAG"
}

# push images to dockerhub
function pushDockerhub() {
  if [[ "${PUSH_DOCKERHUB}" != "true" ]]; then
    echo "skip dockerhub image because PUSH_DOCKERHUB=${PUSH_DOCKERHUB}"
    return
  fi

  DOCKERTAG="${dockerhubaddr}:$1"
  echo "push to dockerhub, image tag: ${DOCKERTAG}"
  args=()
  for arch in "${arches[@]}"
  do
    args+=( --amend "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}" )
  done
  docker manifest create --insecure "$DOCKERTAG" "${args[@]}"
  docker manifest push --insecure "$DOCKERTAG"
}

function isStableVersion() {
    local version=$1
    if [[ $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        return 0  # Stable version
    else
        return 1  # Not a stable version
    fi
}

function isLatestVersion() {
    local version=$1
    git fetch origin 'refs/tags/*:refs/tags/*'
    local latest_version=$(git tag -l --sort=-v:refname | egrep "^v[0-9]+\.[0-9]+\.[0-9]+$" | head -n 1)
    if [[ $version == $latest_version ]]; then
        return 0  # Latest version
    else
        return 1  # Not a latest version
    fi
}

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

if [[ "${PUSH_DOCKERHUB}" == "true" ]]; then
  echo "--- dockerhub login"
  echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin
fi

if [[ -n "${ORIGINAL_IMAGE_TAG+x}" ]] && [[ -n "${NEW_IMAGE_TAG+x}" ]]; then
  echo "--- retag docker image"
  echo "push to gchr, image tag: ${ghcraddr}:${NEW_IMAGE_TAG}"
  args=()
  for arch in "${arches[@]}"
  do
    args+=( --amend "${ghcraddr}:${NEW_IMAGE_TAG}-${arch}" )
  done
  docker manifest create --insecure "${ghcraddr}:${NEW_IMAGE_TAG}" "${args[@]}"
  docker manifest push --insecure "${ghcraddr}:${NEW_IMAGE_TAG}"
  exit 0
fi

echo "--- multi arch image create"
if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
  # If the commit is 40 characters long, it's probably a SHA.
  TAG="git-${BUILDKITE_COMMIT}"
  pushGchr "${TAG}"
fi

if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  # If this is a schedule build, tag the image with the date.
  TAG="nightly-${date}"
  pushGchr "${TAG}"
  pushDockerhub "${TAG}"
  pip install toml-cli
  TAG="v$(toml get --toml-path Cargo.toml workspace.package.version).${date}"
  pushGchr ${TAG}
  pushDockerhub "${TAG}"
  TAG="nightly"
  pushGchr ${TAG}
  pushDockerhub "${TAG}"
fi

# Add a semver tag for non-release branches
if [ "$BUILDKITE_BRANCH" != "main" ] && [[ ! "$BUILDKITE_BRANCH" =~ "^release-.*" ]]; then
  postfix=$(echo ${BUILDKITE_BRANCH} | md5sum | cut -c1-8)
  pip install toml-cli
  TAG="v$(toml get --toml-path Cargo.toml workspace.package.version)-${postfix}"
  pushGchr "${TAG}"
  pushDockerhub "${TAG}"
fi

if [[ -n "${IMAGE_TAG+x}" ]]; then
  # Tag the image with the $IMAGE_TAG.
  TAG="${IMAGE_TAG}"
  pushGchr "${TAG}"
  pushDockerhub "${TAG}"
fi

if [[ -n "${BUILDKITE_TAG}" ]]; then
  # If there's a tag, we tag the image.
  TAG="${BUILDKITE_TAG}"
  pushGchr "${TAG}"
  pushDockerhub "${TAG}"

  if isStableVersion "${TAG}" && isLatestVersion "${TAG}"; then
    # If the tag is a latest stable version, we tag the image with "latest".
    TAG="latest"
    pushGchr "${TAG}"
    pushDockerhub "${TAG}"
  fi
fi

if [[ "${PUSH_DOCKERHUB}" == "true" ]]; then
  echo "--- delete the manifest images from dockerhub"
  args=()
  for arch in "${arches[@]}"
  do
    args+=( "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}" )
  done
  docker run --rm lumir/remove-dockerhub-tag \
    --user "risingwavelabs" --password "$DOCKER_TOKEN" "${args[@]}"
else
  echo "--- skip dockerhub manifest cleanup because PUSH_DOCKERHUB=${PUSH_DOCKERHUB}"
fi
