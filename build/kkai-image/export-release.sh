#!/usr/bin/env bash
set -Eeuo pipefail

readonly IMAGE_REF="${1:?usage: $0 IMAGE_REF VERSION SOURCE_SHA REGISTRY_DIGEST OUTPUT_DIR}"
readonly VERSION="${2:?usage: $0 IMAGE_REF VERSION SOURCE_SHA REGISTRY_DIGEST OUTPUT_DIR}"
readonly SOURCE_SHA="${3:?usage: $0 IMAGE_REF VERSION SOURCE_SHA REGISTRY_DIGEST OUTPUT_DIR}"
readonly REGISTRY_DIGEST="${4:?usage: $0 IMAGE_REF VERSION SOURCE_SHA REGISTRY_DIGEST OUTPUT_DIR}"
readonly OUTPUT_DIR="${5:?usage: $0 IMAGE_REF VERSION SOURCE_SHA REGISTRY_DIGEST OUTPUT_DIR}"

[[ "${IMAGE_REF}" =~ ^ghcr\.io/[a-z0-9._/-]+@sha256:[0-9a-f]{64}$ ]]
[[ "${VERSION}" =~ ^kkai-prod-[0-9]{8}\.[1-9][0-9]*-[0-9a-f]{9}$ ]]
[[ "${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]]
[[ "${REGISTRY_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]]

mkdir -p -- "${OUTPUT_DIR}"
readonly SOURCE_TAG="${IMAGE_REF%@*}:${VERSION}"
readonly ARCHIVE_NAME="invitations-${VERSION}-linux-amd64.tar"
readonly ARCHIVE_PATH="${OUTPUT_DIR}/${ARCHIVE_NAME}"
readonly MANIFEST_PATH="${OUTPUT_DIR}/offline-release.yml"

docker pull "${IMAGE_REF}" >/dev/null
docker tag "${IMAGE_REF}" "${SOURCE_TAG}"
docker image save --output "${ARCHIVE_PATH}" "${SOURCE_TAG}"

ARTIFACT_SHA256="$(sha256sum "${ARCHIVE_PATH}" | awk '{print $1}')"
IMAGE_ID="$(docker image inspect --format '{{.Id}}' "${SOURCE_TAG}")"
ARCHITECTURE="$(docker image inspect --format '{{.Architecture}}' "${SOURCE_TAG}")"
OS_NAME="$(docker image inspect --format '{{.Os}}' "${SOURCE_TAG}")"
IMAGE_USER="$(docker image inspect --format '{{.Config.User}}' "${SOURCE_TAG}")"

[[ "${IMAGE_ID}" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ "${ARCHITECTURE}" == amd64 ]]
[[ "${OS_NAME}" == linux ]]
[[ "${IMAGE_USER}" == 10009:10009 ]]

{
  printf '%s\n' '---' 'schema: 1' 'artifact:'
  printf "  filename: '%s'\n" "${ARCHIVE_NAME}"
  printf "  sha256: '%s'\n" "${ARTIFACT_SHA256}"
  printf '%s\n' 'image:'
  printf "  source_tag: '%s'\n" "${SOURCE_TAG}"
  printf "  mac_manifest_digest: '%s'\n" "${REGISTRY_DIGEST}"
  printf "  remote_image_id: '%s'\n" "${IMAGE_ID}"
  printf "  architecture: '%s'\n" "${ARCHITECTURE}"
  printf "  os: '%s'\n" "${OS_NAME}"
  printf "  user: '%s'\n" "${IMAGE_USER}"
  printf "  source_revision: '%s'\n" "${SOURCE_SHA}"
  printf "  version: '%s'\n" "${VERSION}"
  printf '%s\n' '  rootfs_diff_ids:'
  docker image inspect --format '{{range .RootFS.Layers}}{{println .}}{{end}}' "${SOURCE_TAG}" |
    sed '/^$/d; s/^/    - '\''/; s/$/'\''/'
} >"${MANIFEST_PATH}"

printf '%s  %s\n' "${ARTIFACT_SHA256}" "${ARCHIVE_NAME}" >"${ARCHIVE_PATH}.sha256"
docker image inspect "${SOURCE_TAG}" >"${OUTPUT_DIR}/image-inspect.json"

echo "exported ${ARCHIVE_PATH}"
echo "manifest ${MANIFEST_PATH}"
