#!/usr/bin/env bash

# runs psl weight learning,
# TODO: (Charles) Merge this with the PSL inference script. Lots of repeated code

readonly THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly BASE_DATASET_DIR="${THIS_DIR}/../../psl-datasets"

readonly SUPPORTED_DATASETS='movielens'

# Datasets that cannot use int ids.
readonly STRING_IDS='entity-resolution simple-acquaintances user-modeling'

# Standard options for all datasets and models
# note that this is assuming that we are only using datasets that have int-ids
# todo: (Charles D.) break this assumption
readonly POSTGRES_DB_SERVER='psl_server'
readonly POSTGRES_DB_CLIENT='psl_client'
readonly STANDARD_PSL_OPTIONS="-D admmreasoner.initialconsensusvalue=ZERO -D log4j.threshold=TRACE"


readonly ONLINE_SERVER_PSL_OPTIONS="-D inference.online=true --postgres ${POSTGRES_DB_SERVER} "
readonly ONLINE_CLIENT_PSL_OPTIONS="-D inference.online=true --onlineClient --postgres ${POSTGRES_DB_CLIENT} "

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
    local dataset_name=$1
    local fold=$2
    local time_step=$3
    local evaluator=$4
    local out_directory=$5
    local trace_level=$6

    shift 6

    if [ "$time_step" = "0" ]; then
      echo "Running Online Server"
      start_online_server "$dataset_name" "$fold" "$time_step" "$evaluator" "$out_directory" "$trace_level" "$@" &
      local server_pid=$!
      # TODO: This is so server has time to start up before clients start connecting
      #  (Charles) clients should have their own timeout and we should not have to sleep here
      sleep 8
    fi

    echo "Running online client for fold ${fold} -- time_step ${time_step}"
    run_online_client "$dataset_name" "$fold" "$time_step" "$evaluator" "$out_directory" "$trace_level" "$@"

    if [ "$time_step" = "0" ]; then
        echo "Waiting on Online Server"
        wait ${server_pid}
        echo "Finished Waiting on Online Server"
    fi

    return 0
}

function run_online_client() {
    local dataset_name=$1
    local fold=$2
    local time_step=$3
    local evaluator=$4
    local out_directory=$5
    local trace_level=$6

    local dataset_directory="${BASE_DATASET_DIR}/${dataset_name}"
    local cli_directory="${dataset_directory}/cli"

    # modify runscript to run with the options for this study
    modify_run_script_options "$dataset_directory" "$evaluator" "$trace_level" "$ONLINE_CLIENT_PSL_OPTIONS"

    # modify data files to point to the fold
    provide_command_file "$dataset_directory" "$fold" "$time_step" "$out_directory"

    # reactivate runclientcommands
    reactivate_client "$dataset_directory"

    # deactivate runclientcommands
    deactivate_evaluation "$dataset_directory"

    # run evaluation
    run  "${cli_directory}" "$@"

    # reactivate runclientcommands
    reactivate_evaluation "$dataset_directory"
}

function start_online_server() {
    local dataset_name=$1
    local fold=$2
    local time_step=$3
    local evaluator=$4
    local out_directory=$5
    local trace_level=$6

    local dataset_directory="${BASE_DATASET_DIR}/${dataset_name}"
    local cli_directory="${dataset_directory}/cli"

    # modify runscript to run with the options for this study
    modify_run_script_options "$dataset_directory" "$evaluator" "$trace_level" "$ONLINE_SERVER_PSL_OPTIONS"

    # modify data files to point to the fold
    modify_data_files "$dataset_directory" "$fold" "$time_step"

    # set the psl version for WL experiment
    set_psl_version "$PSL_VERSION" "$dataset_directory"

    # reactivate evaluation
    reactivate_evaluation "$dataset_directory"

    # deactivate runclientcommands
    deactivate_client "$dataset_directory"

    # run evaluation
    run  "${cli_directory}" "$@"

    # reactivate runclientcommands
    reactivate_client "$dataset_directory"

    # modify data files to point back to the 0'th fold
    modify_data_files "$dataset_directory" 0 0
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
        sed -i "s/^readonly ADDITIONAL_PSL_OPTIONS='.*'$/readonly ADDITIONAL_PSL_OPTIONS='${int_ids_options} ${STANDARD_PSL_OPTIONS} -D log4j.threshold=${trace_level}'/" run.sh

        # set the ADDITIONAL_EVAL_OPTIONS
        sed -i "s/^readonly ADDITIONAL_EVAL_OPTIONS='.*'$/readonly ADDITIONAL_EVAL_OPTIONS='--infer=SGDOnlineInference --eval org.linqs.psl.evaluation.statistics.${objective}Evaluator ${DATASET_OPTIONS[${dataset_name}]} ${online_options}'/" run.sh

        # set the ADDITIONAL_CLIENT_OPTIONS
        sed -i "s/^readonly ADDITIONAL_CLIENT_OPTIONS='.*'$/readonly ADDITIONAL_CLIENT_OPTIONS='${online_options}'/" run.sh

    popd > /dev/null
}

function deactivate_client() {
    local dataset_directory=$1
    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    # deactivate weight learning step in run script
    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # deactivate weight learning.
        sed -i 's/^\(\s\+\)runClientCommand/\1# runClientCommand/' run.sh

    popd > /dev/null
}


function reactivate_client() {
    local dataset_directory=$1
    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    # deactivate weight learning step in run script
    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # deactivate weight learning.
        sed -i 's/^\(\s\+\)# runClientCommand/\1runClientCommand/' run.sh

    popd > /dev/null
}

function deactivate_evaluation() {
    local dataset_directory=$1
    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    # deactivate weight learning step in run script
    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # deactivate weight learning.
        sed -i 's/^\(\s\+\)runEvaluation/\1# runEvaluation/' run.sh

    popd > /dev/null
}


function reactivate_evaluation() {
    local dataset_directory=$1
    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    # deactivate weight learning step in run script
    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # deactivate weight learning.
        sed -i 's/^\(\s\+\)# runEvaluation/\1runEvaluation/' run.sh

    popd > /dev/null
}

function provide_command_file() {
    local dataset_directory=$1
    local new_fold=$2
    local time_step=$3
    local output_directory=$4

    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    local command_directory
    command_directory="${dataset_directory}/data/${dataset_name}/${new_fold}/eval/commands"

    # set the output directory for the inferred predicates
    pushd . > /dev/null
        cd "${command_directory}" || exit

        echo "Setting WriteInferredPredicates to ${output_directory} for commands_ts_${time_step}.txt"

        sed -i "s+WriteInferredPredicates.*$+WriteInferredPredicates\\t${output_directory}/inferred-predicates+" "commands_ts_${time_step}.txt"

    popd > /dev/null

    local cli_directory="${dataset_directory}/cli"

    cp "${command_directory}/commands_ts_${time_step}.txt" "${cli_directory}/commands.txt"
}

function modify_data_files() {
    local dataset_directory=$1
    local new_fold=$2
    local time_step=$3

    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    pushd . > /dev/null
        cd "${dataset_directory}/cli" || exit

        # update the fold in the .data file
        sed -i -E "s/${dataset_name}\/[0-9]+\/eval/${dataset_name}\/${new_fold}\/eval/g" "${dataset_name}"-eval.data

        # update the target set in the .data file
        sed -i -E "s/eval\/rating_targets.*\.txt/eval\/rating_targets_ts_${time_step}.txt/g" "${dataset_name}"-eval.data

        # update the truth set in the .data file
        sed -i -E "s/eval\/rating_truth.*\.txt/eval\/rating_truth_ts_${time_step}.txt/g" "${dataset_name}"-eval.data

        # update the observed set in the .data file
        sed -i -E "s/eval\/rating_obs.*\.txt/eval\/rating_obs.txt/g" "${dataset_name}"-eval.data

        # update the rated obs set in the .data file
        sed -i -E "s/eval\/rated_obs.*\.txt/eval\/rated_obs_ts_${time_step}.txt/g" "${dataset_name}"-eval.data

        # update the target obs set in the .data file
        sed -i -E "s/eval\/target_obs.*\.txt/eval\/target_obs_ts_${time_step}.txt/g" "${dataset_name}"-eval.data

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