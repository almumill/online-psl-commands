"""
Helper functions not specific to any experiment
"""

import csv
import os
import sys
import pandas as pd

# evaluator methods
from evaluators import evaluate_accuracy
from evaluators import evaluate_f1
from evaluators import evaluate_roc_auc_score
from evaluators import evaluate_mse

# dict to access the specific evaluator representative needed for weight learning
EVALUATE_METHOD = {'Categorical': evaluate_accuracy,
                   'Discrete': evaluate_f1,
                   'Ranking': evaluate_roc_auc_score,
                   'Continuous': evaluate_mse}

# dict to map the examples to their evaluation predicate
EVAL_PREDICATE = {'citeseer': 'hasCat',
                  'cora': 'hasCat',
                  'epinions': 'trusts',
                  'lastfm': 'rating',
                  'jester': 'rating'}

IS_HIGHER_REP_BETTER = {'Categorical': True,
                        'Discrete': True,
                        'Ranking': True,
                        'Continuous': False}


def load_file(filename):
    output = []

    with open(filename, 'r') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        for line in reader:
            output.append(line)

    return output


def load_user_frame(dataset):
    # path to this file relative to caller
    dirname = os.path.dirname(__file__)

    user_path = "{}/../psl-datasets/{}/data/ml-100k/u.user".format(dirname, dataset)
    user_df = pd.read_csv(user_path, sep='|', header=None,
                          encoding="ISO-8859-1")
    user_df.columns = ['userId', 'age', 'gender', 'occupation', 'zip']
    user_df = user_df.astype({'userId': int})
    user_df = user_df.set_index('userId')

    return user_df


def load_observed_frame(dataset, fold, time_step, predicate, phase='eval'):
    # path to this file relative to caller
    dirname = os.path.dirname(__file__)

    # TODO: (Charles) Notice that this is very specific for the first experiment we are running.
    #  This should be able to be used for both experiments when the observed set does change
    # observed_path = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_obs_ts_{}.txt".format(dirname, dataset, dataset, fold,
    #                                                                               phase, predicate, time_step)

    observed_path = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_obs.txt".format(dirname, dataset, dataset, fold,
                                                                                  phase, predicate)

    observed_df = pd.read_csv(observed_path, sep='\t', header=None)

    # clean up column names and set multi-index for predicate
    arg_columns = ['arg_' + str(col) for col in observed_df.columns[:-1]]
    value_column = ['val']
    observed_df.columns = arg_columns + value_column
    observed_df = observed_df.astype({col: int for col in arg_columns})
    observed_df = observed_df.set_index(arg_columns)

    return observed_df


def load_truth_frame(dataset, fold, time_step, predicate, phase='eval'):
    # path to this file relative to caller
    dirname = os.path.dirname(__file__)

    truth_path = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_truth_ts_{}.txt".format(dirname, dataset, dataset, fold, phase,
                                                                                 predicate, time_step)

    truth_df = pd.read_csv(truth_path, sep='\t', header=None)

    # clean up column names and set multi-index for predicate
    arg_columns = ['arg_' + str(col) for col in truth_df.columns[:-1]]
    value_column = ['val']
    truth_df.columns = arg_columns + value_column
    truth_df = truth_df.astype({col: int for col in arg_columns})
    truth_df = truth_df.set_index(arg_columns)

    # pull out the previous timestep truths
    if eval(time_step) > 0:
        truth_path_prev = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_truth_ts_{}.txt".format(dirname, dataset, dataset, fold,
                                                                                     phase,
                                                                                     predicate, eval(time_step) - 1)
        truth_df_prev = pd.read_csv(truth_path_prev, sep='\t', header=None)

        # clean up column names and set multi-index for predicate
        arg_columns = ['arg_' + str(col) for col in truth_df_prev.columns[:-1]]
        value_column = ['val']
        truth_df_prev.columns = arg_columns + value_column
        truth_df_prev = truth_df_prev.astype({col: int for col in arg_columns})
        truth_df_prev = truth_df_prev.set_index(arg_columns)

        truth_df = truth_df.loc[truth_df.index.difference(truth_df_prev.index)]

    return truth_df


def load_target_frame(dataset, fold, time_step, predicate, phase='eval'):
    # path to this file relative to caller
    dirname = os.path.dirname(__file__)

    # TODO: (Charles D.) This is a hack, there should be a way to find what
    #  the suffix should be for this file from the cli in the psl-examples but its either target or targets
    try:
        target_path = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_target_ts_{}.txt".format(dirname, dataset, dataset, fold,
                                                                                 phase, predicate, time_step)
        target_df = pd.read_csv(target_path, sep='\t', header=None)
    except FileNotFoundError as err:
        target_path = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_targets_ts_{}.txt".format(dirname, dataset, dataset, fold,
                                                                                  phase, predicate, time_step)
        target_df = pd.read_csv(target_path, sep='\t', header=None)
        

    # clean up column names and set multi-index for predicate
    arg_columns = ['arg_' + str(col) for col in target_df.columns]
    target_df.columns = arg_columns
    target_df = target_df.astype({col: int for col in arg_columns})
    target_df = target_df.set_index(arg_columns)

    # pull out the previous timestep truths
    if eval(time_step) > 0:
        target_path_prev = "{}/../psl-datasets/{}/data/{}/{}/{}/{}_targets_ts_{}.txt".format(dirname, dataset, dataset, fold,
                                                                                     phase,
                                                                                     predicate, eval(time_step) - 1)
        target_df_prev = pd.read_csv(target_path_prev, sep='\t', header=None)

        # clean up column names and set multi-index for predicate
        arg_columns = ['arg_' + str(col) for col in target_df_prev.columns[:-1]]
        value_column = ['val']
        target_df_prev.columns = arg_columns + value_column
        target_df_prev = target_df_prev.astype({col: int for col in arg_columns})
        target_df_prev = target_df_prev.set_index(arg_columns)

        target_df = target_df.loc[target_df.index.difference(target_df_prev.index)]

    return target_df


def load_wrapper_args(args):
    executable = args.pop(0)
    if len(args) < 8 or ({'h', 'help'} & {arg.lower().strip().replace('-', '') for arg in args}):
        print("USAGE: python3 {} <srl method name> <evaluator name> <example_name> <fold> <seed> <study> <out_directory>... <additional inference script args>".format(
            executable), file=sys.stderr)
        sys.exit(1)

    srl_method_name = args.pop(0)
    evaluator_name = args.pop(0)
    example_name = args.pop(0)
    fold = args.pop(0)
    seed = args.pop(0)
    alpha = eval(args.pop(0))
    study = args.pop(0)
    out_directory = args.pop(0)

    return srl_method_name, evaluator_name, example_name, fold, seed, alpha, study, out_directory