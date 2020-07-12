import pandas as pd
import numpy as np
import os

from command_constructors.create_commands import df_to_command
from command_constructors.command_utils import command_file_write
import command_constructors.constants as constants
from predicate_constructors.ratings import ratings_predicate
from predicate_constructors.rated import rated_predicate
from predicate_constructors.nmf_ratings import nmf_ratings_predicate
from predicate_constructors.nb_ratings import nb_ratings_predicate
from predicate_constructors.sim_content import sim_content_predicate
from predicate_constructors.avg_item_rating import average_item_rating_predicate
from predicate_constructors.avg_user_rating import average_user_rating_predicate
from predicate_constructors.sim_users import sim_users_predicate
from predicate_constructors.sim_demo_users import sim_demo_users_predicate
from predicate_constructors.sim_items import sim_items_predicate
from predicate_constructors.target import target_predicate

DATA_PATH = "../psl-datasets/movielens/data"
RATING_CONSTANT_COL_NAMES = ['userId', 'movieId']
RATING_PREDICATE_NAME = 'rating'
RATED_PREDICATE_NAME = 'rated'
TARGET_PREDICATE_NAME = 'target'
RATING_VALUE_COL_NAME = 'rating'
N_MONTHS_PER_FOLD = 4
N_FOLDS = 5


def construct_movielens_predicates():
    """
    Create data directory to write output to
    """
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    """
    Assuming that the raw data already exists in the data directory
    """
    movies_df, ratings_df, user_df = load_dataframes()
    movies_df, ratings_df, user_df = filter_dataframes(movies_df, ratings_df, user_df)
    # TODO: Random samples of 70%
    ratings_df_list = partition_by_timestamp(ratings_df)

    for fold, fold_ratings_df in enumerate(ratings_df_list):
        out_directory = DATA_PATH + '/movielens/' + str(fold) + '/eval'
        print("Making " + out_directory)
        if not os.path.exists(out_directory):
            os.makedirs(out_directory)

        # get meta data for this fold
        fold_user_df = user_df.loc[fold_ratings_df.index.get_level_values('userId').unique()]
        # filter movies in movie df
        fold_movies_df = movies_df.loc[fold_ratings_df.index.get_level_values('movieId').unique()]

        # get observations/truth split for this fold
        # TODO: Train Validate Test split for weight.
        fold_ratings_df_list = partition_by_timestamp(fold_ratings_df, n_partitions=3)
        observed_ratings_df = fold_ratings_df_list[0]
        truth_ratings_df = pd.concat(fold_ratings_df_list[1:])
        partitioned_truth_ratings = partition_by_timestamp(truth_ratings_df, n_partitions=20)

        # construct predicates that are static for fold
        construct_static_predicates(observed_ratings_df, truth_ratings_df, fold_movies_df, fold_user_df, fold)

        # construct predicates that are dynamic with time
        construct_dynamic_predicates(observed_ratings_df, partitioned_truth_ratings, fold)

        print("Did fold #"+str(fold))


def construct_dynamic_predicates(observed_ratings_df, partitioned_truth_ratings, fold):
    ratings_predicate(observed_ratings_df, partition='obs', fold=str(fold))
    aggregated_observed_ratings = observed_ratings_df
    for time_step in np.arange(len(partitioned_truth_ratings)):
        truth_ratings_df_i = partitioned_truth_ratings[time_step]
        if time_step > 0:
            aggregated_observed_ratings = aggregated_observed_ratings.append(partitioned_truth_ratings[time_step - 1],
                                                                             ignore_index=False)

        # construct and write the predicates for timestamp
        ratings_predicate(aggregated_observed_ratings, partition='obs_ts_' + str(time_step), fold=str(fold))
        ratings_predicate(truth_ratings_df_i, partition='targets_ts_' + str(time_step), fold=str(fold),
                          write_value=False)
        ratings_predicate(truth_ratings_df_i, partition='truth_ts_' + str(time_step), fold=str(fold))

        rated_predicate(aggregated_observed_ratings, truth_ratings_df_i,
                        partition='obs_ts_' + str(time_step), fold=str(fold))
        target_predicate(truth_ratings_df_i, partition='obs_ts_' + str(time_step), fold=str(fold))

        # construct client commands
        command_list = []
        delete_target_command_list = []
        add_targets_command_list = []
        add_observation_command_list = []
        extra_commands = [constants.WRITE_INFERRED_COMMAND]

        if time_step > 0:
            # write time step command file
            cur_truth_ratings_args = truth_ratings_df_i.reset_index().loc[:, RATING_CONSTANT_COL_NAMES]

            # add and delete ratings atoms
            add_targets_command_list = df_to_command(cur_truth_ratings_args, cur_truth_ratings_args.loc[:, []],
                                                     constants.ADD, constants.TARGET, RATING_PREDICATE_NAME)
            prev_timestamp_ratings = partitioned_truth_ratings[time_step - 1].reset_index()
            delete_target_command_list += df_to_command(prev_timestamp_ratings.loc[:, RATING_CONSTANT_COL_NAMES],
                                                        prev_timestamp_ratings.loc[:, []],
                                                        constants.DELETE, constants.TARGET, RATING_PREDICATE_NAME)
            add_observation_command_list += df_to_command(prev_timestamp_ratings.loc[:, RATING_CONSTANT_COL_NAMES],
                                                          prev_timestamp_ratings.loc[:, [RATING_VALUE_COL_NAME]],
                                                          constants.ADD, constants.OBS, RATING_PREDICATE_NAME)

            # add rated atoms
            new_rated_df = truth_ratings_df_i.reset_index()
            add_observation_command_list += df_to_command(new_rated_df.loc[:, RATING_CONSTANT_COL_NAMES],
                                                          new_rated_df.loc[:, [RATING_VALUE_COL_NAME]].clip(1, 1),
                                                          constants.ADD, constants.OBS, RATED_PREDICATE_NAME)

            # add target atoms
            add_observation_command_list += df_to_command(new_rated_df.loc[:, RATING_CONSTANT_COL_NAMES],
                                                          new_rated_df.loc[:, [RATING_VALUE_COL_NAME]].clip(1, 1),
                                                          constants.ADD, constants.OBS, TARGET_PREDICATE_NAME)

            if time_step == len(partitioned_truth_ratings) - 1:
                extra_commands += [constants.CLOSE_COMMAND]

        extra_commands += [constants.EXIT_COMMAND]

        command_list += (add_targets_command_list + delete_target_command_list +
                         add_observation_command_list + extra_commands)

        command_file_write(command_list, str(fold), 'eval',
                           "commands_ts_" + str(time_step) + '.txt')


def construct_static_predicates(observed_ratings_df, truth_ratings_df, movies_df, users_df, fold):
    users = users_df.index.unique()
    movies = movies_df.index.unique()

    nmf_ratings_predicate(observed_ratings_df, truth_ratings_df, str(fold))
    nb_ratings_predicate(observed_ratings_df, truth_ratings_df, users_df, movies_df, str(fold))

    # TODO (Alex): Should be dynamic
    average_item_rating_predicate(observed_ratings_df, truth_ratings_df, str(fold))
    average_user_rating_predicate(observed_ratings_df, truth_ratings_df, str(fold))

    sim_content_predicate(movies_df, str(fold))
    sim_demo_users_predicate(users_df, str(fold))
    sim_items_predicate(observed_ratings_df, truth_ratings_df, movies, str(fold))
    sim_users_predicate(observed_ratings_df, truth_ratings_df, users, str(fold))


def partition_by_timestamp(ratings_df, n_partitions=N_FOLDS):
    sorted_frame = ratings_df.sort_values(by='timestamp')
    return np.array_split(sorted_frame, n_partitions, axis=0)


def split_by_timestamp(ratings_df, train_proportion=0.3):
    sorted_frame = ratings_df.sort_values(by='timestamp')
    return (sorted_frame.iloc[: int(sorted_frame.shape[0] * train_proportion), :],
            sorted_frame.iloc[int(sorted_frame.shape[0] * train_proportion):, :])


def filter_dataframes(movies_df, ratings_df, user_df, n=50):
    """
    Get rid of users who have not yet rated more than n movies.
    """
    # filter users that have less than n ratings
    filtered_ratings_df = ratings_df.groupby('userId').filter(lambda x: x.shape[0] > n)
    # filter ratings by users have dont have demographic information
    filtered_ratings_df = filtered_ratings_df.reindex(user_df.index, level='userId').dropna(axis='index')

    # filter users in user df that did not have n ratings
    filtered_user_df = user_df.loc[filtered_ratings_df.index.get_level_values('userId').unique()]
    # filter movies in movie df
    filtered_movies_df = movies_df.loc[filtered_ratings_df.index.get_level_values('movieId').unique()]

    # TODO: (Charles) Testing Purposes
    # filtered_ratings_df = filtered_ratings_df.sample(100)
    return filtered_movies_df, filtered_ratings_df, filtered_user_df


def load_dataframes():
    """
    Assuming that the raw data already exists in the data directory
    """
    movies_df = pd.read_csv(DATA_PATH + "/ml-1m/movies.dat", sep='::', header=None, encoding="ISO-8859-1",
                            engine='python')
    movies_df.columns = ["movieId", "movie title", "genres"]
    movies_df = movies_df.join(movies_df["genres"].str.get_dummies('|')).drop('genres', axis=1)
    movies_df = movies_df.astype({'movieId': int})
    movies_df = movies_df.set_index('movieId')

    ratings_df = pd.read_csv(DATA_PATH + '/ml-1m/ratings.dat', sep='::', header=None, engine='python')
    ratings_df.columns = ['userId', 'movieId', 'rating', 'timestamp']
    ratings_df = ratings_df.astype({'userId': int, 'movieId': int})
    ratings_df.rating = ratings_df.rating / ratings_df.rating.max()
    ratings_df = ratings_df.set_index(['userId', 'movieId'])

    user_df = pd.read_csv(DATA_PATH + '/ml-1m/users.dat', sep='::', header=None,
                          encoding="ISO-8859-1", engine='python')
    user_df.columns = ['userId', 'gender', 'age', 'occupation', 'zip']
    user_df = user_df.astype({'userId': int})
    user_df = user_df.set_index('userId')

    return movies_df, ratings_df, user_df


def main():
    construct_movielens_predicates()


if __name__ == '__main__':
    main()
