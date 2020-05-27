#!/usr/bin/env bash

# Run all the experiments.

PSL_ONLINE_DATASETS='movielens'

function main() {
    trap exit SIGINT

    # dataset paths to pass to scripts
    psl_dataset_paths=''
    for dataset in $PSL_ONLINE_DATASETS; do
        psl_dataset_paths="${psl_dataset_paths}psl-datasets/${dataset} "
    done

    # PSL Experiments
    # Fetch the data and models if they are not already present and make some
    # modifications to the run scripts and models.
    ./scripts/psl_scripts/setup_psl_datasets.sh

    echo "Running psl performance experiments on datasets: [${PSL_ONLINE_DATASETS}]."
    pushd . > /dev/null
        cd "./scripts" || exit
        # shellcheck disable=SC2086
        ./run_online_performance_experiments.sh "psl" ${psl_dataset_paths}
    popd > /dev/null
}

main "$@"