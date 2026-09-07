#!/usr/bin/env bash

################################### SCRIPT BOILERPLATE

set -euo pipefail

source ci/scripts/common.sh
unset RUSTC_WORKSPACE_WRAPPER

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

# profile is either ci-dev or ci-release
if [[ "$profile" == "ci-dev" ]]; then
  echo "Running in ci-dev mode"
elif [[ "$profile" == "ci-release" ]]; then
  echo "Running in ci-release mode"
else
    echo "Invalid option: profile must be either ci-dev or ci-release" 1>&2
    exit 1
fi

source e2e_test/backwards-compat-tests/scripts/utils.sh

################################### Main

configure_rw() {
VERSION="$1"
ENABLE_BUILD="$2"
# Whether to build the Java connector-node from source during `risedev d`.
# Defaults to true for backwards compatibility; callers that have already
# provided a pre-built connector (via download) should pass "false".
BUILD_CONNECTOR="${3:-true}"

echo "--- Setting up cluster config"
  if version_le "$VERSION" "1.8.9"; then
    cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: sqlite
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
EOF
  else
     # For versions >= 1.9.0, we have support for different sql meta-backend,
     # so we need to specify the meta-backend: sqlite
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
EOF
  fi

cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=true

ENABLE_MINIO=true

# Whether to build or directly fetch binary from release.
ENABLE_BUILD_RUST=$ENABLE_BUILD

# Use target/debug for simplicity.
ENABLE_RELEASE_PROFILE=false
ENABLE_UDF=true
EOF

# cargo-make's `env_set` predicate only checks that the variable is defined,
# regardless of value — so we must omit ENABLE_BUILD_RW_CONNECTOR entirely when
# we want `build-connector-node` to be skipped.
if [[ "$BUILD_CONNECTOR" == "true" ]]; then
  echo "ENABLE_BUILD_RW_CONNECTOR=true" >> risedev-components.user.env
fi

# See https://github.com/risingwavelabs/risingwave/pull/15448
if version_le "${VERSION:-}" "1.8.0" ; then
  echo "ENABLE_ALL_IN_ONE=true" >> risedev-components.user.env
fi
}

# Try to download the pre-built Java connector-node tarball for a specific
# release from GitHub. Returns 0 on success (connector extracted and
# CONNECTOR_LIBS_PATH exported), non-zero if the release asset is unavailable.
download_release_connector_artifact() {
  local version="$1"
  local tarball="risingwave-connector-v${version}.tar.gz"
  local url="https://github.com/risingwavelabs/risingwave/releases/download/v${version}/${tarball}"
  echo "--- Try downloading pre-built Java connector for $version"
  rm -f "$tarball"
  if ! wget --no-verbose "$url" -O "$tarball"; then
    rm -f "$tarball"
    return 1
  fi
  rm -rf ./connector-node
  mkdir -p ./connector-node
  tar xf "$tarball" -C ./connector-node
  export CONNECTOR_LIBS_PATH="$(pwd)/connector-node/libs"
  return 0
}

setup_release_cluster() {
  local version="$1"

  echo "--- Build risedev for $version, it may not be backwards compatible"
  git config --global --add safe.directory /risingwave
  # `common.sh` exports GIT_SHA=$BUILDKITE_COMMIT (the PR tip). After we
  # check out an old tag, vergen's VERGEN_GIT_SHA reflects the old commit,
  # and the compile-time assertion in src/cmd_all/src/bin/risingwave.rs
  # panics because the two SHAs disagree. Drop GIT_SHA so the old build
  # only relies on VERGEN_GIT_SHA.
  unset GIT_SHA
  git checkout "v${version}"
  cargo build -p risedev
  echo "--- Get RisingWave binary for $version"
  local archive="risingwave-v${version}-x86_64-unknown-linux.tar.gz"
  local url="https://github.com/risingwavelabs/risingwave/releases/download/v${version}/${archive}"
  local enable_build_rust="false"
  rm -f "$archive"
  if ! wget --no-verbose "$url" -O "$archive"; then
    rm -f "$archive"
    echo "Failed to download ${version} from github releases, build from source later during \`risedev d\`"
    enable_build_rust="true"
  elif [[ $version = '1.10.0' || $version = '1.10.1' || $version = '1.10.2' || $version = '2.0.0' ]]; then
    echo "1.10.x, 2.0.0 have dynamically linked openssl, build from source later during \`risedev d\`"
    enable_build_rust="true"
  else
    rm -f risingwave
    tar -xvf "$archive"
    mv risingwave target/debug/risingwave

    echo "--- Prepared cluster binary from tag $version"
    git config --global --add safe.directory /risingwave
  fi

  # Try to reuse the pre-built Java connector from the old release. Falls back
  # to building from source (inside `risedev d`) when the asset is unavailable.
  local build_connector="true"
  if download_release_connector_artifact "$version"; then
    build_connector="false"
  else
    echo "Pre-built Java connector for $version not found, build from source later during \`risedev d\`"
  fi

  configure_rw "$version" "$enable_build_rust" "$build_connector"
}

setup_old_cluster() {
  setup_release_cluster "$OLD_VERSION"
}

upgrade_through_intermediate_versions() {
  local version

  while read -r version; do
    [[ -z "$version" ]] && continue

    echo "--- Upgrade through intermediate version $version"
    setup_release_cluster "$version"
    rm -rf .risingwave/config
    ENABLE_UDF=1 ./risedev d full-without-monitoring
    if version_le "$RECOVER_COMMAND_MIN_VERSION" "$version"; then
      wait_for_recovery "$version"
    fi
    kill_cluster
  done < <(get_intermediate_versions)
}

setup_new_cluster() {
  echo "--- Setup Risingwave @ $RW_COMMIT"
  git checkout "$RW_COMMIT"
  download_and_prepare_rw "$profile" common
  # Reuse the Java connector artifact produced by the `build-other` step
  # instead of rebuilding it from source during `risedev d`.
  download_connector_node_artifact
  # Make sure we always start w/o old config
  rm -r .risingwave/config
}

main() {
  set -euo pipefail
  # Make sure we have all the branches
  git fetch --all
  get_rw_versions

  setup_old_cluster
  seed_old_cluster "$OLD_VERSION"

  upgrade_through_intermediate_versions

  setup_new_cluster
  # Assume we use the latest version, so we just set to some large number.
  # The current $NEW_VERSION as of this change is 1.7.0, so we can't use that.
  # See: https://github.com/risingwavelabs/risingwave/pull/15448
  configure_rw "99.99.99" false false
  validate_new_cluster "$NEW_VERSION"
}

main
