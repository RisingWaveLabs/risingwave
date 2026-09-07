#!/usr/bin/env bash
set -euo pipefail

# Download and extract the official ADBC Snowflake driver shared library.
#
# This script is intentionally shared by:
# - RiseDev task `download-adbc-snowflake` (src/risedevtool/adbc.toml)
# - Docker image build (docker/Dockerfile)
#
# Inputs (env vars):
# - ADBC_DRIVER_VERSION: ADBC Driver Foundry release version (default: 1.12.0)
# - DEST_DIR: where to place the extracted shared library (default: "${PWD}/.risingwave/bin/adbc")
# - TMP_DIR: temp directory for the downloaded archive (default: "${PWD}/.risingwave/tmp")
#
# Outputs:
# - ${DEST_DIR}/libadbc_driver_snowflake.so (Linux) or .dylib (macOS)

ADBC_DRIVER_VERSION="${ADBC_DRIVER_VERSION:-1.12.0}"
DEST_DIR="${DEST_DIR:-"${PWD}/.risingwave/bin/adbc"}"
TMP_DIR="${TMP_DIR:-"${PWD}/.risingwave/tmp"}"

# ADBC Driver Foundry does not publish Intel macOS artifacts. Keep using the
# last Apache Arrow ADBC release that supports this development platform.
LEGACY_ADBC_RELEASE_VERSION="23"
LEGACY_ADBC_DRIVER_VERSION="1.11.0"

OS_TYPE="$(uname -s)"
ARCH_TYPE="$(uname -m)"

LIB_SUFFIX=""
ARCHIVE_PLATFORM=""
USE_LEGACY_WHEEL="false"

case "${OS_TYPE}" in
  Linux)
    LIB_SUFFIX="so"
    case "${ARCH_TYPE}" in
      x86_64)
        ARCHIVE_PLATFORM="linux_amd64"
        ;;
      aarch64)
        ARCHIVE_PLATFORM="linux_arm64"
        ;;
      *)
        echo "Error: Unsupported Linux architecture: ${ARCH_TYPE}" >&2
        exit 1
        ;;
    esac
    ;;
  Darwin)
    LIB_SUFFIX="dylib"
    case "${ARCH_TYPE}" in
      x86_64)
        USE_LEGACY_WHEEL="true"
        ;;
      arm64)
        ARCHIVE_PLATFORM="macos_arm64"
        ;;
      *)
        echo "Error: Unsupported macOS architecture: ${ARCH_TYPE}" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "Error: Unsupported operating system: ${OS_TYPE}" >&2
    exit 1
    ;;
esac

DRIVER_NAME="libadbc_driver_snowflake.${LIB_SUFFIX}"

if [ "${USE_LEGACY_WHEEL}" = "true" ]; then
  ARTIFACT_FILENAME="adbc_driver_snowflake-${LEGACY_ADBC_DRIVER_VERSION}-py3-none-macosx_10_15_x86_64.whl"
  DOWNLOAD_URL="https://github.com/apache/arrow-adbc/releases/download/apache-arrow-adbc-${LEGACY_ADBC_RELEASE_VERSION}/${ARTIFACT_FILENAME}"
  SOURCE_DRIVER_NAME="libadbc_driver_snowflake.so"
  DOWNLOAD_DESCRIPTION="Apache Arrow ADBC ${LEGACY_ADBC_RELEASE_VERSION} (driver ${LEGACY_ADBC_DRIVER_VERSION}, Intel macOS fallback)"
else
  ARTIFACT_FILENAME="snowflake_${ARCHIVE_PLATFORM}_v${ADBC_DRIVER_VERSION}.tar.gz"
  DOWNLOAD_URL="https://github.com/adbc-drivers/snowflake/releases/download/go/v${ADBC_DRIVER_VERSION}/${ARTIFACT_FILENAME}"
  SOURCE_DRIVER_NAME="${DRIVER_NAME}"
  DOWNLOAD_DESCRIPTION="ADBC Driver Foundry Snowflake driver ${ADBC_DRIVER_VERSION}"
fi

if [ -f "${DEST_DIR}/${DRIVER_NAME}" ]; then
  exit 0
fi

echo "ADBC Snowflake driver not found, downloading ${DOWNLOAD_DESCRIPTION}"
echo "Platform: ${OS_TYPE} ${ARCH_TYPE}"
echo "Download URL: ${DOWNLOAD_URL}"

mkdir -p "${DEST_DIR}" "${TMP_DIR}"
curl -fL -o "${TMP_DIR}/${ARTIFACT_FILENAME}" "${DOWNLOAD_URL}"

# Intel macOS still uses the legacy wheel. Current releases use tar archives.
if [ "${USE_LEGACY_WHEEL}" = "true" ]; then
  unzip -j -o "${TMP_DIR}/${ARTIFACT_FILENAME}" "adbc_driver_snowflake/${SOURCE_DRIVER_NAME}" -d "${DEST_DIR}"
else
  tar -xzf "${TMP_DIR}/${ARTIFACT_FILENAME}" -C "${DEST_DIR}" "${SOURCE_DRIVER_NAME}"
fi
if [ "${SOURCE_DRIVER_NAME}" != "${DRIVER_NAME}" ]; then
  mv "${DEST_DIR}/${SOURCE_DRIVER_NAME}" "${DEST_DIR}/${DRIVER_NAME}"
fi
rm -f "${TMP_DIR}/${ARTIFACT_FILENAME}"

if [ ! -f "${DEST_DIR}/${DRIVER_NAME}" ]; then
  echo "Error: ADBC Snowflake driver file not found after extraction" >&2
  echo "DEST_DIR contents:" >&2
  ls -la "${DEST_DIR}/" >&2 || true
  exit 1
fi

echo "ADBC Snowflake driver installed successfully at ${DEST_DIR}/${DRIVER_NAME}"
