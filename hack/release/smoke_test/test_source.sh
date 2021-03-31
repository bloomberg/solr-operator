#!/usr/bin/env bash
# exit immediately when a command fails
set -e
# only exit with zero if all commands of the pipeline exit successfully
set -o pipefail
# error on unset variables
set -u

show_help() {
cat << EOF
Usage: ./hack/release/smoke_test/test_source.sh [-h] -v VERSION -l LOCATION

Test the source bundle artifact

    -h  Display this help and exit
    -v  Version of the Solr Operator
    -l  Base location of the staged artifacts. Can be a URL or relative or absolute file path.
EOF
}

OPTIND=1
while getopts hv:l: opt; do
    case $opt in
        h)
            show_help
            exit 0
            ;;
        v)  VERSION=$OPTARG
            ;;
        l)  LOCATION=$OPTARG
            ;;
        *)
            show_help >&2
            exit 1
            ;;
    esac
done
shift "$((OPTIND-1))"   # Discard the options and sentinel --

if [[ -z "${VERSION:-}" ]]; then
  error "Specify a project version through -v, or through the VERSION env var"; exit 1
fi
if [[ -z "${LOCATION:-}" ]]; then
  error "Specify an base artifact location -l, or through the LOCATION env var"; exit 1
fi

TMP_DIR=$(mktemp -d --tmpdir "solr-operator-smoke-test-source-XXXXXXXX")

# If LOCATION is not a URL, then get the absolute path
if ! (echo "${LOCATION}" | grep -E "http"); then
  LOCATION=$(cd "${LOCATION}"; pwd)
fi

echo "Download source artifact, verify and run 'make check'"
# Do all logic in temporary directory
(
  cd "${TMP_DIR}"

  if (echo "${LOCATION}" | grep -E "http"); then
    # Download source
    wget "${LOCATION}/source/solr-operator-${VERSION}.tgz"

    # Pull docker image, since we are working with remotely staged artifacts
    docker pull "apache/solr-operator:${TAG}"
  else
    cp "${LOCATION}/source/solr-operator-${VERSION}.tgz" .
  fi

  # Unpack the source code
  tar -xzf "solr-operator-${VERSION}.tgz"
  cd "solr-operator-${VERSION}"

  # Install the dependencies
  make install-dependencies

  # Run the checks
  make check

  # Check the version
  FOUND_VERSION=$(make version)
  if [[ "$FOUND_VERSION" != "${VERSION}" ]]; then
    error "Version in source release should be ${VERSION}, but found ${FOUND_VERSION}"
    exit 1
  fi

  # Check the docker image for License & Notice info
  # TODO
)

# Delete temporary source directory
rm -rf "${TMP_DIR}"

echo "Source verification successful!"