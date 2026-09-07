#!/usr/bin/env bash

set -euo pipefail

ORIGINAL_BRANCH=$(git branch --show-current)

on_exit() {
  git checkout "$ORIGINAL_BRANCH"
}

trap on_exit EXIT

source e2e_test/backwards-compat-tests/scripts/utils.sh

configure_rw() {
  VERSION="$1"

  echo "--- Setting up cluster config"
  if version_le "$VERSION" "1.9.0"; then
    cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: etcd
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
    - use: kafka
      user-managed: true
      address: message_queue
      port: 29092
EOF
  elif version_lt "$VERSION" "$HUMMOCK_STALE_TABLE_IDS_MIN_VERSION"; then
    cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: etcd
    - use: meta-node
      meta-backend: etcd
    - use: compute-node
    - use: frontend
    - use: compactor
    - use: kafka
      user-managed: true
      address: message_queue
      port: 29092
EOF
  else
    cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: sqlite
    - use: meta-node
      meta-backend: sqlite
    - use: compute-node
    - use: frontend
    - use: compactor
    - use: kafka
      user-managed: true
      address: message_queue
      port: 29092
EOF
  fi

cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=false

ENABLE_MINIO=true

# Fetch risingwave binary from release.
ENABLE_BUILD_RUST=true

# ENABLE_RELEASE_PROFILE=true
EOF
}

setup_old_cluster() {
  echo "--- Setting up old cluster"
  LATEST_BRANCH=$(git branch --show-current)
  git checkout "v${OLD_VERSION}"
}

setup_new_cluster() {
  echo "--- Setting up new cluster"
  rm -r .risingwave/bin/risingwave
  git checkout $LATEST_BRANCH
}

upgrade_through_intermediate_versions() {
  local version

  while read -r version; do
    [[ -z "$version" ]] && continue

    echo "--- Upgrade through intermediate version $version"
    rm -rf .risingwave/bin/risingwave
    git checkout "v${version}"
    configure_rw "$version"
    rm -rf .risingwave/config
    ENABLE_UDF=1 ./risedev d full-without-monitoring
    if version_le "$RECOVER_COMMAND_MIN_VERSION" "$version"; then
      wait_for_recovery "$version"
    fi
    kill_cluster
  done < <(get_intermediate_versions)
}

main() {
  set -euo pipefail
  get_rw_versions
  setup_old_cluster
  configure_rw "$OLD_VERSION"
  seed_old_cluster "$OLD_VERSION"

  upgrade_through_intermediate_versions

  setup_new_cluster
  configure_rw "99.99.99"
  validate_new_cluster "$NEW_VERSION"
}

main
