#!/usr/bin/env bash
# exit immediately when a command fails
set -e
# only exit with zero if all commands of the pipeline exit successfully
set -o pipefail
# error on unset variables
set -u

show_help() {
cat << EOF
Usage: ./hack/release/artifacts/create_artifacts.sh [-h] [-v VERSION] [-g GPG_KEY] [-a APACHE_ID] -d ARTIFACTS_DIR

Setup the release of all artifacts, then create signatures.

    -h  Display this help and exit
    -v  Version of the Solr Operator (Optional, will default to project version)
    -d  Base directory of the staged artifacts.
    -g  GPG Key to use when signing artifacts (Optional)
    -a  Apache ID, to use when signing the helm chart (Optional)
EOF
}

OPTIND=1
while getopts hv:g:a:d: opt; do
    case $opt in
        h)
            show_help
            exit 0
            ;;
        v)  VERSION=$OPTARG
            ;;
        g)  GPG_KEY=$OPTARG
            ;;
        a)  APACHE_ID=$OPTARG
            ;;
        d)  ARTIFACTS_DIR=$OPTARG
            ;;
        *)
            show_help >&2
            exit 1
            ;;
    esac
done
shift "$((OPTIND-1))"   # Discard the options and sentinel --

if [[ -z "${VERSION:-}" ]]; then
  VERSION=$(make version)
fi
if [[ -z "${ARTIFACTS_DIR:-}" ]]; then
  error "Specify an base artifact directory -d, or through the ARTIFACTS_DIR env var"; exit 1
fi

GPG_USER=()
if [[ -n "${GPG_KEY:-}" ]]; then
  GPG_USER=(-u "${GPG_KEY}")
fi
APACHE_ID_PASS_THROUGH=()
if [[ -n "${APACHE_ID:-}" ]]; then
  APACHE_ID_PASS_THROUGH=(-a "${APACHE_ID}")
fi

echo "Setting up Solr Operator ${VERSION} release artifacts at '${ARTIFACTS_DIR}'"

./hack/release/artifacts/bundle_source.sh -d "${ARTIFACTS_DIR}" -v "${VERSION}"
./hack/release/artifacts/create_crds.sh -d "${ARTIFACTS_DIR}" -v "${VERSION}"
./hack/release/artifacts/build_helm.sh -d "${ARTIFACTS_DIR}" -v "${VERSION}" "${APACHE_ID_PASS_THROUGH[@]}"

# Generate signature and checksum for every file
(
  cd "${ARTIFACTS_DIR}"

  for artifact_directory in $(find * -type d); do
    (
      cd "${artifact_directory}"

      for artifact in $(find * -type f ! \( -name '*.asc' -o -name '*.sha512' -o -name '*.prov' \) ); do
        if [ ! -f "${artifact}.asc" ]; then
          gpg "${GPG_USER[@]}" -ab "${artifact}"
        fi
        if [ ! -f "${artifact}.sha512" ]; then
          sha512sum -b "${artifact}" > "${artifact}.sha512"
        fi
      done
    )
  done
)