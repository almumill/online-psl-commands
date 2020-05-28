#!/usr/bin/env bash

# runs psl weight learning,

readonly THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly BASE_DATASET_DIR="${THIS_DIR}/../../psl-datasets"

readonly SUPPORTED_DATASETS='movielens'

# Datasets that cannot use int ids.
readonly STRING_IDS='entity-resolution simple-acquaintances user-modeling'

# Standard options for all datasets and models
# note that this is assuming that we are only using datasets that have int-ids
# todo: (Charles D.) break this assumption
readonly POSTGRES_DB='psl'
readonly STANDARD_PSL_OPTIONS="--postgres ${POSTGRES_DB} -D admmreasoner.initialconsensusvalue=ZERO -D log4j.threshold=TRACE"


readonly ONLINE_SERVER_PSL_OPTIONS="inference.online=true"
readonly ONLINE_CLIENT_PSL_OPTIONS="inference.online=true --onlineClient"

# Options specific to each dataset (missing keys yield empty strings).
declare -A DATASET_OPTIONS
DATASET_OPTIONS[movielens]=''

readonly PSL_VERSION='2.3.0-SNAPSHOT'

function run() {
    local cli_directory=$1

    pushd . > /dev/null
        cd "${cli_directory}" || exit
        ./run.sh "$@"
    popd > /dev/null
}

function run_inference() {
    if [ "$time_step" = "0" ]; then
      run_online_server "$@"
    else
      run_online_client "$@"
    fi

    return 0
}

function run_online_server() {
    local dataset_name=$1
    local fold=$2
    local time_step=$3
    local evaluator=$4
    local out_directory=$5
    local trace_level=$6

    shift 6

    # Write the command file for the specific timestamp


    # pipe commands to psl cli to send off to server

    # query results

    # save results
}

function run_online_server() {
    local dataset_name=$1
    local fold=$2
    local time_step=$3
    local evaluator=$4
    local out_directory=$5
    local trace_level=$6

    shift 6

    local dataset_directory="${BASE_DATASET_DIR}/${dataset_name}"
    local cli_directory="${dataset_directory}/cli"

    # modify runscript to run with the options for this study
    modify_run_script_options "$dataset_directory" "$evaluator" "$trace_level" "$ONLINE_SERVER_PSL_OPTIONS"

    # modify data files to point to the fold
    modify_data_files "$dataset_directory" "$fold"

    # set the psl version for WL experiment
    set_psl_version "$PSL_VERSION" "$dataset_directory"

    # run evaluation
    run  "${cli_directory}" "$@"

    # modify data files to point back to the 0'th fold
    modify_data_files "$dataset_directory" 0

    # save inferred predicates
    mv "${cli_directory}/inferred-predicates" "${out_directory}/inferred-predicates"
}

function set_psl_version() {
    local psl_version=$1
    local dataset_directory=$2

    pushd . > /dev/null
      cd "${dataset_directory}/cli"

      # Set the PSL version.
      sed -i "s/^readonly PSL_VERSION='.*'$/readonly PSL_VERSION='${psl_version}'/" run.sh

    popd > /dev/null
}

function modify_run_script_options() {
    local dataset_directory=$1
    local objective=$2
    local trace_level=$3
    local online_options=$4

    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    local int_ids_options=''
    # Check for int ids.
    if [[ "${STRING_IDS}" != *"${dataset_name}"* ]]; then
        int_ids_options="--int-ids ${int_ids_options}"
    fi

    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # set the ADDITIONAL_PSL_OPTIONS
        sed -i "s/^readonly ADDITIONAL_PSL_OPTIONS='.*'$/readonly ADDITIONAL_PSL_OPTIONS='${int_ids_options} ${STANDARD_PSL_OPTIONS}'/" run.sh

        # set the ADDITIONAL_EVAL_OPTIONS
        sed -i "s/^readonly ADDITIONAL_EVAL_OPTIONS='.*'$/readonly ADDITIONAL_EVAL_OPTIONS='--infer=OnlineInference --eval org.linqs.psl.evaluation.statistics.${objective}Evaluator -D log4j.threshold=${trace_level} ${DATASET_OPTIONS[${dataset_name}]} ${online_options}'/" run.sh
    popd > /dev/null
}

function modify_data_files() {
    local dataset_directory=$1
    local new_fold=$2

    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # update the fold in the .data file
        sed -i -E "s/${dataset_name}\/[0-9]+\/eval/${dataset_name}\/${new_fold}\/eval/g" "${dataset_name}"-eval.data
    popd > /dev/null
}

function main() {
    if [[ $# -le 5 ]]; then
        echo "USAGE: $0 <dataset_name> <fold> <time_step> <evaluator> <out directory> <TRACE_LEVEL>"
        echo "USAGE: Datasets can be among: ${SUPPORTED_DATASETS}"
        exit 1
    fi

    trap exit SIGINT

    run_inference "$@"
}

main "$@"