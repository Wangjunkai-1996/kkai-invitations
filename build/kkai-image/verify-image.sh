#!/usr/bin/env bash
set -Eeuo pipefail

readonly IMAGE_REF="${1:?usage: $0 IMAGE_REF}"
readonly EXPECTED_SHA="${EXPECTED_SHA:-}"
readonly EXPECTED_VERSION="${EXPECTED_VERSION:-}"
readonly EXPECTED_REGISTRY_DIGEST="${EXPECTED_REGISTRY_DIGEST:-}"

[[ "${EXPECTED_SHA}" =~ ^[0-9a-f]{40}$ ]] || {
  echo "EXPECTED_SHA must be an explicit 40-character commit" >&2
  exit 64
}
[[ "${EXPECTED_VERSION}" =~ ^kkai-prod-[0-9]{8}\.[1-9][0-9]*-[0-9a-f]{9}$ ]] || {
  echo "EXPECTED_VERSION must be an immutable production version" >&2
  exit 64
}

ARCHITECTURE="$(docker image inspect --format '{{.Architecture}}' "${IMAGE_REF}")"
OS_NAME="$(docker image inspect --format '{{.Os}}' "${IMAGE_REF}")"
IMAGE_USER="$(docker image inspect --format '{{.Config.User}}' "${IMAGE_REF}")"
IMAGE_SHA="$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "${IMAGE_REF}")"
IMAGE_VERSION="$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "${IMAGE_REF}")"
IMAGE_ID="$(docker image inspect --format '{{.Id}}' "${IMAGE_REF}")"
ROOTFS_DIFF_IDS="$(docker image inspect --format '{{range .RootFS.Layers}}{{println .}}{{end}}' "${IMAGE_REF}")"

[[ "${ARCHITECTURE}" == amd64 ]]
[[ "${OS_NAME}" == linux ]]
[[ "${IMAGE_USER}" == 10009:10009 ]]
[[ "${IMAGE_SHA}" == "${EXPECTED_SHA}" ]]
[[ "${IMAGE_VERSION}" == "${EXPECTED_VERSION}" ]]
[[ "${IMAGE_ID}" =~ ^sha256:[0-9a-f]{64}$ ]]
[[ -n "${ROOTFS_DIFF_IDS}" ]]

if [[ -n "${EXPECTED_REGISTRY_DIGEST}" ]]; then
  [[ "${EXPECTED_REGISTRY_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]]
  docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${IMAGE_REF}" |
    grep -F "@${EXPECTED_REGISTRY_DIGEST}" >/dev/null
fi

docker run --rm --pull=never --platform linux/amd64 \
  --entrypoint /usr/local/bin/invitations-entrypoint "${IMAGE_REF}" --self-test |
  grep -Fx 'invitations-entrypoint: ok' >/dev/null

set +e
PREFLIGHT_OUTPUT="$(
  docker run --rm --pull=never --platform linux/amd64 \
    --entrypoint /usr/local/bin/invitations "${IMAGE_REF}" --preflight 2>&1
)"
PREFLIGHT_STATUS=$?
set -e
[[ "${PREFLIGHT_STATUS}" -eq 3 ]]
grep -F 'invalid_invitations_database_configuration' <<<"${PREFLIGHT_OUTPUT}" >/dev/null

echo "verified ${IMAGE_REF}"
echo "image ID: ${IMAGE_ID}"
