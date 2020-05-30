#!/usr/bin/env bash

# Fetch the PSL examples and modify the CLI configuration for these experiments.
# Note that you can change the version of PSL used with the PSL_VERSION option in the run inference and run wl scripts.

readonly BASE_DIR=$(realpath "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..)
readonly PSL_DATASET_DIR="${BASE_DIR}/psl-datasets"
readonly RESOURCES_DIR="${BASE_DIR}/psl_resources"

readonly PSL_VERSION="2.2.1"
readonly JAR_PATH="./psl-cli-${PSL_VERSION}.jar"


#readonly AVAILABLE_MEM_KB=$(cat /proc/meminfo | grep 'MemTotal' | sed 's/^[^0-9]\+\([0-9]\+\)[^0-9]\+$/\1/')
## Floor by multiples of 5 and then reserve an additional 5 GB.
#readonly JAVA_MEM_GB=$((${AVAILABLE_MEM_KB} / 1024 / 1024 / 5 * 5 - 5))
readonly JAVA_MEM_GB=8

function fetch_jar() {
    # Only make a new resources directory if it does not already exist
    [[ -d "$RESOURCES_DIR" ]] || mkdir -p "$RESOURCES_DIR"

    # psl 2.2.1
    local remoteJARURL="https://repo1.maven.org/maven2/org/linqs/psl-cli/2.2.1/psl-cli-2.2.1.jar"
    echo "${remoteJARURL} ${JAR_PATH}"
    wget "${remoteJARURL}" "${JAR_PATH}" 'psl-jar'
    mv psl-cli-2.2.1.jar "${RESOURCES_DIR}/psl-cli-2.2.1.jar"

    # psl 2.3.0
    local snapshotJARPath="$HOME/.m2/repository/org/linqs/psl-cli/2.3.0-SNAPSHOT/psl-cli-2.3.0-SNAPSHOT.jar"
    cp "${snapshotJARPath}" "${RESOURCES_DIR}/psl-cli-2.3.0-SNAPSHOT.jar"
#    wget -q https://linqs-data.soe.ucsc.edu/public/SRLWeightLearning/psl-cli-2.3.0-SNAPSHOT.jar
#    mv psl-cli-2.3.0-SNAPSHOT.jar "${RESOURCES_DIR}/psl-cli-2.3.0-SNAPSHOT.jar"
}

# Special fixes for select examples.
function special_fixes() {
  echo "No Special Fixes"
}

# Common to all examples.
function standard_fixes() {
    for dataset_dir in `find ${PSL_DATASET_DIR} -maxdepth 1 -mindepth 1 -type d -not -name '.*' -not -name '_scripts'`; do
        local baseName=`basename ${dataset_dir}`

        pushd . > /dev/null
            cd "${dataset_dir}/cli"

            # Increase memory allocation.
            echo "Increasing memory allocation"
            sed -i "s/java -jar/java -Xmx${JAVA_MEM_GB}G -Xms${JAVA_MEM_GB}G -jar/" run.sh

            # cp 2.2.1
            cp "${RESOURCES_DIR}/psl-cli-2.2.1.jar" ./

            # cp 2.3.0 snapshot into the cli directory
            cp "${RESOURCES_DIR}/psl-cli-2.3.0-SNAPSHOT.jar" ./

            # Deactivate fetch psl step
            echo "Deactivating fetch psl step"
            sed -i 's/^\(\s\+\)fetch_psl/\1# fetch_psl/' run.sh

        popd > /dev/null

    done
}

function create_postgres_db() {
  echo "INFO: Creating psl_server postgres user and db..."
  psql postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='psl_server'" | grep -q 1 || createuser -s psl_server
  psql postgres -lqt | cut -d \| -f 1 | grep -qw psl_server || createdb psl_server

  echo "INFO: Creating psl_client postgres user and db..."
  psql postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='psl_client'" | grep -q 1 || createuser -s psl_client
  psql postgres -lqt | cut -d \| -f 1 | grep -qw psl_client || createdb psl_client
}

function main() {
   trap exit SIGINT

   fetch_jar
   create_postgres_db
   special_fixes
   standard_fixes

   exit 0
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"