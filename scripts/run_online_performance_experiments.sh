#!/usr/bin/env bash

# run online performance experiments,
#i.e. collects runtime and evaluation statistics of various weight learning methods

readonly THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly BASE_DIR="${THIS_DIR}/.."
readonly BASE_OUT_DIR="${BASE_DIR}/results/online"

#readonly ONLINE_METHODS='psl online_psl'
readonly ONLINE_METHODS='online_psl'
readonly TRACE_LEVEL='TRACE'

# set of currently supported examples
readonly SUPPORTED_DATASETS='movielens'

# Evaluators to be use for each example
declare -A DATASET_EVALUATORS
DATASET_EVALUATORS[movielens]='Continuous'

# Evaluators to be use for each example
# todo: (Charles D.) just read this information from psl example data directory rather than hardcoding
declare -A DATASET_FOLDS
DATASET_FOLDS[movielens]=8

declare -A DATASET_TIME_STEPS
DATASET_TIME_STEPS[movielens]=10


function run_example() {
    local dataset_directory=$1
    local online_method=$2
    local evaluator=$3
    local fold=$4
    local time_step=$5

    local dataset_name
    dataset_name=$(basename "${dataset_directory}")

    out_directory="${BASE_OUT_DIR}/${online_method}/performance_study/${dataset_name}/${evaluator}/${fold}/${time_step}"

    # Only make a new out directory if it does not already exist
    [[ -d "$out_directory" ]] || mkdir -p "$out_directory"

    ##### EVALUATION #####
    echo "Running ${online_method} ${dataset_name} (fold${fold} -- time_step${time_step})."

    # path to output files
    local out_path="${out_directory}/eval_out.txt"
    local err_path="${out_directory}/eval_out.err"
    local time_path="${out_directory}/eval_time.txt"

    if [[ -e "${out_path}" ]]; then
        echo "Output file already exists, skipping: ${out_path}"
    else
        # call inference script for online method type
        pushd . > /dev/null
            cd "${online_method}_scripts" || exit
#            /usr/bin/time -v --output="${time_path}" ./run_inference.sh "${dataset_name}" "${fold}" "${time_step}" "${evaluator}" "${out_directory}" "${TRACE_LEVEL}" > "$out_path" 2> "$err_path"
            if [ "${time_step}" = "0"  ] && [ "${online_method}" = "online_psl" ]; then
              echo "Running online_psl in background"
              ./run_inference.sh "${dataset_name}" "${fold}" "${time_step}" "${evaluator}" "${out_directory}" "${TRACE_LEVEL}" > "$out_path" 2> "$err_path" &
              local server_pid=$!
              # TODO: This is so server has time to start up before clients start connecting
              #  (Charles) clients should have their own timeout and we should not have to sleep here
              sleep 10
            else
              ./run_inference.sh "${dataset_name}" "${fold}" "${time_step}" "${evaluator}" "${out_directory}" "${TRACE_LEVEL}" > "$out_path" 2> "$err_path"
            fi
        popd > /dev/null
    fi

    if [ "${time_step}" = "$(( ${DATASET_TIME_STEPS[${dataset_name}]} - 1 ))" ] && [ "${online_method}" = "online_psl" ]; then
      echo "Waiting on server for fold ${fold}"
      wait ${server_pid}
    fi

    return 0
}

function main() {
    trap exit SIGINT

    if [[ $# -le 0 ]]; then
        echo "USAGE: $0 <example dir> ..."
        echo "USAGE: Example Directories can be among: ${SUPPORTED_DATASETS}"
        exit 1
    fi

    local dataset_name

    for dataset_directory in "$@"; do
        for online_method in ${ONLINE_METHODS}; do
            dataset_name=$(basename "${dataset_directory}")
            for evaluator in ${DATASET_EVALUATORS[${dataset_name}]}; do
                for ((fold=0; fold<${DATASET_FOLDS[${dataset_name}]}; fold++)) do
                    for ((time_step=0; time_step<${DATASET_TIME_STEPS[${dataset_name}]}; time_step++)) do
                        run_example "${dataset_directory}" "${online_method}" "${evaluator}" "${fold}" "${time_step}"
                    done
                done
            done
        done
    done

    return 0
}

main "$@"
