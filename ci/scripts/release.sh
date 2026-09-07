#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

SKIP_RELEASE=${SKIP_RELEASE:-0}
REPO_ROOT=${PWD}
ARCH="$(uname -m)"

# By default, we use `thin-production` for binary release.
# This includes only symbol tables but no debug info, aiming for a small binary size.
CARGO_PROFILE=${CARGO_PROFILE:-thin-production}

echo "--- Check env"
if [ "${BUILDKITE_SOURCE}" != "schedule" ] && [ "${BUILDKITE_SOURCE}" != "webhook" ] && [[ -z "${BINARY_NAME+x}" ]]; then
  exit 0
fi

echo "--- Install aws cli"
curl "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip && ./aws/install && mv /usr/local/bin/aws /bin/aws

echo "--- Install lld"
dnf install -y lld
ld.lld --version

echo "--- Install dependencies"
dnf install -y perl-core wget python3.12 python3.12-devel cyrus-sasl-devel rsync openssl-devel blas-devel lapack-devel libgomp
# python udf compiling requires python3.12
update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.12 3

echo "--- Install java and maven"
dnf install -y java-21-openjdk java-21-openjdk-devel
pipx install toml-cli
wget --no-verbose https://rw-ci-deps-dist.s3.amazonaws.com/apache-maven-3.9.3-bin.tar.gz && tar -zxvf apache-maven-3.9.3-bin.tar.gz
export PATH="${REPO_ROOT}/apache-maven-3.9.3/bin:$PATH"
mvn -v

echo "--- Install rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
source "$HOME/.cargo/env"
rustup show

echo "--- Install sccache"
curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
cargo binstall -y --locked sccache@0.10.0

echo "--- Install protoc3"
PROTOC_ARCH=${ARCH}
if [ "${ARCH}" == "aarch64" ]; then
  # shellcheck disable=SC1068
  PROTOC_ARCH="aarch_64"
fi
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-"${PROTOC_ARCH}".zip
unzip -o protoc-3.15.8-linux-"${PROTOC_ARCH}".zip -d protoc
mv ./protoc/bin/protoc /usr/local/bin/
mv ./protoc/include/* /usr/local/include/

echo "--- Install nodejs"
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
export NVM_DIR="$HOME/.nvm"
. "$NVM_DIR/nvm.sh"
cd dashboard && nvm install && nvm use && cd ..
npm install --global pnpm@10.14.0

echo "--- Check risingwave release version"
if [[ -n "${BUILDKITE_TAG}" ]]; then
  CARGO_PKG_VERSION="$(toml get --toml-path Cargo.toml workspace.package.version)"
  if [[ "${CARGO_PKG_VERSION}" != "${BUILDKITE_TAG#*v}" ]]; then
    echo "CARGO_PKG_VERSION: ${CARGO_PKG_VERSION}"
    echo "BUILDKITE_TAG: ${BUILDKITE_TAG}"
    echo "CARGO_PKG_VERSION and BUILDKITE_TAG are not equal"
    exit 1
  fi
fi

echo "--- Build risingwave release binary"
source ci/scripts/common.sh
unset RUSTC_WORKSPACE_WRAPPER # disable rustc-workspace-wrapper, for coverage instrumentation

export ENABLE_BUILD_DASHBOARD=1
if [ "${ARCH}" == "aarch64" ]; then
  # enable large page size support for jemalloc
  # see https://github.com/tikv/jemallocator/blob/802969384ae0c581255f3375ee2ba774c8d2a754/jemalloc-sys/build.rs#L218
  export JEMALLOC_SYS_WITH_LG_PAGE=16
fi

cargo build -p risingwave_cmd_all --features "rw-static-link" --features udf --features datafusion --features openssl-vendored --profile "${CARGO_PROFILE}"
cargo build -p risingwave_cmd --bin risectl --features "rw-static-link" --features openssl-vendored --profile "${CARGO_PROFILE}"

echo "--- Check link info"
check_link_info "${CARGO_PROFILE}"

echo "--- Show sccache stats"
sccache --show-stats
sccache --zero-stats

echo "--- Check binary size"
cd target/"${CARGO_PROFILE}" && chmod +x risingwave risectl
du -sh risingwave* risectl

if [ "${SKIP_RELEASE}" -ne 1 ]; then
  echo "--- Upload nightly binary to s3"
  if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
    tar -czvf risingwave-"$(date '+%Y%m%d')"-"${ARCH}"-unknown-linux.tar.gz risingwave
    aws s3 cp risingwave-"$(date '+%Y%m%d')"-"${ARCH}"-unknown-linux.tar.gz s3://rw-nightly-pre-built-binary
  elif [[ -n "${BINARY_NAME+x}" ]]; then
    tar -czvf risingwave-"${BINARY_NAME}"-"${ARCH}"-unknown-linux.tar.gz risingwave
    aws s3 cp risingwave-"${BINARY_NAME}"-"${ARCH}"-unknown-linux.tar.gz s3://rw-nightly-pre-built-binary
  fi
else
  echo "--- Skipped upload nightly binary"
fi

echo "--- Build connector node"
cd "${REPO_ROOT}"/java && mvn -B package -Dmaven.test.skip=true -Dno-build-rust

if [[ -n "${BUILDKITE_TAG}" ]]; then
  echo "--- Collect all release assets"
  cd "${REPO_ROOT}" && mkdir release-assets && cd release-assets
  cp -r "${REPO_ROOT}"/target/"${CARGO_PROFILE}"/* .
  mv "${REPO_ROOT}"/java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz risingwave-connector-"${BUILDKITE_TAG}".tar.gz
  tar -zxvf risingwave-connector-"${BUILDKITE_TAG}".tar.gz libs
  ls -l

  echo "--- Install gh cli"
  dnf install -y 'dnf-command(config-manager)'
  dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
  dnf install -y gh

  if [ "${SKIP_RELEASE}" -ne 1 ]; then
    echo "--- Release create"
    set +e
    response=$(gh release view -R risingwavelabs/risingwave "${BUILDKITE_TAG}" 2>&1)
    set -euo pipefail
    if [[ $response == *"not found"* ]]; then
      echo "Tag ${BUILDKITE_TAG} does not exist. Creating release..."
      gh release create "${BUILDKITE_TAG}" --notes "release ${BUILDKITE_TAG}" -d -p
    else
      echo "Tag ${BUILDKITE_TAG} already exists. Skipping release creation."
    fi

    echo "--- Release upload risingwave asset"
    tar -czvf risingwave-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux.tar.gz risingwave
    gh release upload --clobber "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux.tar.gz

    echo "--- Release upload risingwave debug info"
    # Some cargo profiles may not generate split debug info (e.g. no `risingwave.dwp`).
    if [[ -f risingwave.dwp ]]; then
      tar -czvf risingwave-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux.dwp.tar.gz risingwave.dwp
      gh release upload --clobber "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux.dwp.tar.gz
    else
      echo "No risingwave.dwp found; skipping debug info upload."
    fi

    echo "--- Release upload risectl asset"
    tar -czvf risectl-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux.tar.gz risectl
    gh release upload --clobber "${BUILDKITE_TAG}" risectl-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux.tar.gz

    connector_assets=$(gh release view "${BUILDKITE_TAG}" --json assets --jq '.assets[] | select(.name | contains("risingwave-connector"))' | wc -l)
    if [[ ${connector_assets} -eq 0 ]]; then
      echo "--- Release upload connector libs asset"
      tar -czvf risingwave-connector-"${BUILDKITE_TAG}".tar.gz libs
      gh release upload --clobber "${BUILDKITE_TAG}" risingwave-connector-"${BUILDKITE_TAG}".tar.gz
    fi

    echo "--- Release upload risingwave-all-in-one asset"
    tar -czvf risingwave-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux-all-in-one.tar.gz risingwave libs
    gh release upload --clobber "${BUILDKITE_TAG}" risingwave-"${BUILDKITE_TAG}"-"${ARCH}"-unknown-linux-all-in-one.tar.gz
  else
    echo "--- Skipped upload RW assets"
  fi
fi
