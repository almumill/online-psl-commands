#!/usr/bin/env bash

# Fetch the PSL examples and modify the CLI configuration for these experiments.
# Note that you can change the version of PSL used with the PSL_VERSION option in the run inference and run wl scripts.

readonly BASE_DIR=$(realpath "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../..)

readonly PSL_VERSION="2.2.1"
readonly JAR_PATH="./psl-cli-${PSL_VERSION}.jar"

readonly SPECIALIZED_EXAMPLES_DIR="${BASE_DIR}/specialized-examples"

readonly AVAILABLE_MEM_KB=$(cat /proc/meminfo | grep 'MemTotal' | sed 's/^[^0-9]\+\([0-9]\+\)[^0-9]\+$/\1/')
# Floor by multiples of 5 and then reserve an additional 5 GB.
readonly JAVA_MEM_GB=$((${AVAILABLE_MEM_KB} / 1024 / 1024 / 5 * 5 - 5))
#readonly JAVA_MEM_GB=8

function fetch_jar() {
    # psl 2.2.1
    local remoteJARURL="https://repo1.maven.org/maven2/org/linqs/psl-cli/2.2.1/psl-cli-2.2.1.jar"
    wget "${remoteJARURL}" "${JAR_PATH}" 'psl-jar'
    mv psl-cli-2.2.1.jar psl_resources/psl-cli-2.2.1.jar

    # psl 2.3.0
    local snapshotJARPath="$HOME/.m2/repository/org/linqs/psl-cli/2.3.0-SNAPSHOT/psl-cli-2.3.0-SNAPSHOT.jar"
    cp "${snapshotJARPath}" psl_resources/psl-cli-2.3.0-SNAPSHOT.jar
#    wget -q https://linqs-data.soe.ucsc.edu/public/SRLWeightLearning/psl-cli-2.3.0-SNAPSHOT.jar
#    mv psl-cli-2.3.0-SNAPSHOT.jar psl_resources/psl-cli-2.3.0-SNAPSHOT.jar
}

# Special fixes for select examples.
function special_fixes() {
  pass
}

# Common to all examples.
function standard_fixes() {
    echo "$BASE_DIR"
    for exampleDir in `find ${PSL_EXAMPLES_DIR} -maxdepth 1 -mindepth 1 -type d -not -name '.*' -not -name '_scripts'`; do
        local baseName=`basename ${exampleDir}`

        pushd . > /dev/null
            cd "${exampleDir}/cli"

            # Increase memory allocation.
            sed -i "s/java -jar/java -Xmx${JAVA_MEM_GB}G -Xms${JAVA_MEM_GB}G -jar/" run.sh

            # cp 2.2.1
            cp ../../../psl_resources/psl-cli-2.2.1.jar ./

            # cp 2.3.0 snapshot into the cli directory
            cp ../../../psl_resources/psl-cli-2.3.0-SNAPSHOT.jar ./

            # Deactivate fetch psl step
            sed -i 's/^\(\s\+\)fetch_psl/\1# fetch_psl/' run.sh

        popd > /dev/null

    done
}

function main() {
   trap exit SIGINT

   fetch_jar
   special_fixes
   standard_fixes

   exit 0
}

[[ "${BASH_SOURCE[0]}" == "${0}" ]] && main "$@"