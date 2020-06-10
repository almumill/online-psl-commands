#!/usr/bin/python
import pandas as pd
import numpy as np
import os

# generic helpers
from helpers import load_truth_frame
from helpers import load_observed_frame
from helpers import load_target_frame

# helpers for experiment specific processing
from psl_scripts.helpers import load_prediction_frame as load_psl_prediction_frame

# evaluators implemented for this study
from evaluators import evaluate_accuracy
from evaluators import evaluate_f1
from evaluators import evaluate_mse
from evaluators import evaluate_roc_auc_score

dataset_properties = {'movielens': {'evaluation_predicate': 'rating'}}

evaluator_name_to_method = {
    'Categorical': evaluate_accuracy,
    'Discrete': evaluate_f1,
    'Continuous': evaluate_mse,
    'Ranking': evaluate_roc_auc_score
}

TIMING_COLUMNS = ['Dataset', 'Inference_Method', 'Mean_Wall_Clock_Time', 'Wall_Clock_Time_Time_Standard_Deviation']
PERFORMANCE_COLUMNS = ['Dataset', 'Inference_Method', 'Evaluation_Method', 'Time_Step', 'Mean', 'Standard_Error']

DIRNAME = os.path.dirname(__file__)

def main():
    # in results/online/{}/performance_study write
    # a performance.csv file with columns 
    # Dataset | Inference_Method | Evaluation_Method | Time_Step | Mean | Standard_Error


    # we are going to overwrite the file with all the most up to date information
    # timing_frame = pd.DataFrame(columns=TIMING_COLUMNS)
    performance_frame = pd.DataFrame(columns=PERFORMANCE_COLUMNS)

    # extract all the files that are in the results directory
    # path to this file relative to caller
    path = '{}/../results/online/performance_study'.format(DIRNAME)
    inference_methods = [inference_method for inference_method in os.listdir(path) if
                         os.path.isdir(os.path.join(path, inference_method))]

    for inference_method in inference_methods:
        path = '{}/../results/online/performance_study/{}'.format(DIRNAME, inference_method)
        datasets = [dataset for dataset in os.listdir(path) if os.path.isdir(os.path.join(path, dataset))]

        print("-----------------------------------------------------------------------------------")
        print(inference_method)

        # iterate over all datasets adding the results to the performance_frame
        for dataset in datasets:
            # extract all the metrics that are in the directory
            path = '{}/../results/online/performance_study/{}/{}'.format(DIRNAME, inference_method, dataset)
            evaluators = [evaluator for evaluator in os.listdir(path) if os.path.isdir(os.path.join(path, evaluator))]

            for evaluator in evaluators:
                # extract all the folds that are in the directory
                path = '{}/../results/online/performance_study/{}/{}/{}'.format(DIRNAME, inference_method,
                                                                                dataset, evaluator)
                folds = [fold for fold in os.listdir(path) if os.path.isdir(os.path.join(path, fold))]

                # calculate experiment performance and append to performance frame
                # this is the average performance for each time step across the folds
                performance_df = calculate_experiment_performance(dataset, inference_method, evaluator, folds)
                performance_frame = performance_frame.append(performance_df, ignore_index=True)

                # # calculate experiment timing and append to timing frame
                # timing_series = calculate_experiment_timing(dataset, evaluator, folds)
                # timing_frame = timing_frame.append(timing_series, ignore_index=True)


    # write performance_frame and timing_frame to results/weightlearning/{}/performance_study
    performance_frame.to_csv(
        '{}/../results/online/performance_study/performance.csv'.format(DIRNAME),
        index=False)
    # timing_frame.to_csv(
    #     '{}/../results/weightlearning/{}/performance_study/{}_timing.csv'.format(dirname, method, method),
    #     index=False)


# def calculate_experiment_timing(dataset, wl_method, evaluator, folds):
#     dirname = os.path.dirname(__file__)
#
#     # initialize the experiment_timing_frame that will be populated in the following for loop
#     experiment_timing_frame = pd.DataFrame(columns=['wall_clock_seconds'])
#
#     for fold in folds:
#         path = '{}/../results/weightlearning/{}/performance_study/{}/{}/{}/{}'.format(
#             dirname, method, dataset, wl_method, evaluator, fold
#         )
#         # load the timing data
#         try:
#             # timing series for fold
#             cmd = "tail -n 1 " + path + "/learn_out.txt | cut -d ' ' -f 1"
#             output = subprocess.getoutput(cmd)
#             try:
#                 time_seconds = int(output) / 1000
#             except ValueError as _:
#                 time_seconds = 0
#
#             fold_timing_series = pd.Series(data=time_seconds, index=experiment_timing_frame.columns)
#             # add fold timing to experiment timing
#             experiment_timing_frame = experiment_timing_frame.append(fold_timing_series, ignore_index=True)
#         except (FileNotFoundError, pd.errors.EmptyDataError, KeyError) as err:
#             print('{}: {}'.format(path, err))
#             continue
#
#     # parse the timing series
#     timing_series = pd.Series(index=TIMING_COLUMNS,
#                               dtype=float)
#     experiment_timing_frame = experiment_timing_frame.astype({'wall_clock_seconds': float})
#     timing_series['Dataset'] = dataset
#     timing_series['Wl_Method'] = wl_method
#     timing_series['Evaluation_Method'] = evaluator
#     timing_series['Mean_Wall_Clock_Time'] = experiment_timing_frame['wall_clock_seconds'].mean()
#     timing_series['Wall_Clock_Time_Time_Standard_Deviation'] = experiment_timing_frame['wall_clock_seconds'].std()
#
#     return timing_series


def calculate_experiment_performance(dataset, inference_method, evaluator, folds):
    # initialize the experiment list that will be populated in the following for
    # loop with the performance outcome of each fold
    fold_performance = np.array([])
    for fold in folds:
        experiment_performance = np.array([])

        path = '{}/../results/online/performance_study/{}/{}/{}/{}'.format(DIRNAME, inference_method,
                                                                           dataset, evaluator, fold)

        time_steps = [time_step for time_step in os.listdir(path) if os.path.isdir(os.path.join(path, fold))]
        for time_step in time_steps:
            # load the prediction dataframe
            try:
                predicted_df = load_psl_prediction_frame(dataset, inference_method, evaluator, fold, time_step,
                                                         dataset_properties[dataset]['evaluation_predicate'],
                                                         "performance_study")
            except FileNotFoundError as err:
                print(err)
                continue

            # truth dataframe
            truth_df = load_truth_frame(dataset, fold, time_step, dataset_properties[dataset]['evaluation_predicate'])
            # observed dataframe
            observed_df = load_observed_frame(dataset, fold, time_step, dataset_properties[dataset]['evaluation_predicate'])
            # target dataframe
            target_df = load_target_frame(dataset, fold, time_step, dataset_properties[dataset]['evaluation_predicate'])

            # experiment_performance will be a np array with experiment performance values indexed by the time step
            experiment_performance = np.append(experiment_performance,
                                               evaluator_name_to_method[evaluator](predicted_df,
                                                                                   truth_df,
                                                                                   observed_df,
                                                                                   target_df))
        if fold_performance.shape[0] == 0:
            fold_performance = np.array([experiment_performance])
        else:
            fold_performance = np.append(fold_performance, [experiment_performance], axis=0)

    # organize into a performance_dataframe
    performance_df = pd.DataFrame(columns=PERFORMANCE_COLUMNS, index=time_steps)
    performance_df['Dataset'] = dataset
    performance_df['Inference_Method'] = inference_method
    performance_df['Evaluation_Method'] = evaluator
    performance_df['Time_Step'] = time_steps
    performance_df['Mean'] = fold_performance.mean(axis=0)
    performance_df['Standard_Error'] = fold_performance.std(axis=0, ddof=1)

    return performance_df


if __name__ == '__main__':
    main()
