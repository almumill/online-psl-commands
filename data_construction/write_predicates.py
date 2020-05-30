import pandas as pd
import numpy as np
import os

from helpers import get_months_list, get_month_and_date, timestamp_matches_month, standardize_ratings
from command_constructors.create_commands import df_to_command
from command_constructors.command_utils import command_file_write
import command_constructors.constants as constants
from predicate_constructors.ratings import ratings_predicate
from predicate_constructors.rated import rated_predicate
from predicate_constructors.nmf_ratings import nmf_ratings_predicate
from predicate_constructors.sim_content import sim_content_predicate
from predicate_constructors.avg_item_rating import average_item_rating_predicate
from predicate_constructors.avg_user_rating import average_user_rating_predicate
from predicate_constructors.user import user_predicate
from predicate_constructors.item import item_predicate
from predicate_constructors.sim_users import sim_users_predicate
from predicate_constructors.sim_items import sim_items_predicate
from predicate_constructors.target import target_predicate

DATA_PATH = "../psl-datasets/movielens/data/"
RATING_CONSTANT_COL_NAMES = ['userId', 'movieId']
RATING_PREDICATE_NAME = 'rating'
RATED_PREDICATE_NAME = 'rated'
TARGET_PREDICATE_NAME = 'target'
RATING_VALUE_COL_NAME = 'rating'


def construct_movielens_predicates():
    """
    """

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
    movies_df_dict, ratings_df_dict, user_df_dict, time_split_keys = split_by_months(movies_df, ratings_df, user_df)

    ordered_list_of_months = get_months_list(ratings_df)

    for i in range(len(ordered_list_of_months)):
        if not os.path.exists(DATA_PATH + '/movielens/' + str(i) + '/eval'):
            os.makedirs(DATA_PATH + '/movielens/' + str(i) + '/eval')

        # get observations/truths for this split
        fold_ratings_df = ratings_df_dict[ordered_list_of_months[i]]
        observed_ratings_df, truth_ratings_df = split_by_timestamp(fold_ratings_df)
        standardized_observed_ratings_df, standardized_truth_ratings_df = standardize_ratings(observed_ratings_df, truth_ratings_df)
        user_df = user_df_dict[ordered_list_of_months[i]]
        movies_df = movies_df_dict[ordered_list_of_months[i]]
        partitioned_truth_ratings = parition_by_timestamp(standardized_truth_ratings_df)
        users = fold_ratings_df.reset_index().userId.unique()
        movies = fold_ratings_df.reset_index().movieId.unique()

        # construct predicates that are static for fold
        nmf_ratings_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, str(i))
        item_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, str(i))
        user_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, str(i))
        average_item_rating_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, str(i))
        average_user_rating_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, str(i))
        sim_content_predicate(movies_df, str(i))
        sim_items_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, movies, str(i))
        sim_users_predicate(standardized_observed_ratings_df, standardized_truth_ratings_df, users, str(i))

        # construct predicates that are dynamic with time
        ratings_predicate(standardized_observed_ratings_df, partition='obs', fold=str(i))
        aggregated_truths = pd.DataFrame()
        for time_step, truth_ratings_df_i in enumerate(partitioned_truth_ratings):
            aggregated_truths = aggregated_truths.append(truth_ratings_df_i, ignore_index=False)

            # construct and write the observations for timestamp
            rated_predicate(standardized_observed_ratings_df, aggregated_truths,
                            partition='obs_ts_' + str(time_step), fold=str(i))
            target_predicate(aggregated_truths, partition='obs_ts_' + str(time_step), fold=str(i))

            # construct and write the target for timestamp
            ratings_predicate(aggregated_truths, partition='targets_ts_' + str(time_step), fold=str(i),
                              write_value=False)
            ratings_predicate(aggregated_truths, partition='truth_ts_' + str(time_step), fold=str(i))

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
            command_file_write(add_targets_command_list + delete_target_command_list + add_observation_command_list +
                               extra_commands, str(i), 'eval',
                               "commands_ts_" + str(time_step) + '.txt')

        print("Did fold #"+str(i))
    for i in range(len(ordered_list_of_months)):
        print("Fold #"+str(i)+" -- " + str(ordered_list_of_months[i]))


def split_by_timestamp(ratings_df, train_proportion=0.8):
    sorted_frame = ratings_df.sort_values(by='timestamp')
    return (sorted_frame.iloc[: int(sorted_frame.shape[0] * train_proportion), :],
            sorted_frame.iloc[int(sorted_frame.shape[0] * train_proportion):, :])


def parition_by_timestamp(ratings_df, n_paritions=10):
    sorted_frame = ratings_df.sort_values(by='timestamp')
    integer_paritions = np.array_split(np.arange(sorted_frame.shape[0]), n_paritions)
    return [sorted_frame.iloc[i] for i in integer_paritions]


def filter_dataframes(movies_df, ratings_df, user_df):
    """
    Note that there are no users where there are less than 20 ratings occurring in the raw datatset
    """
    return movies_df, ratings_df, user_df


def load_dataframes():
    """
    Assuming that the raw data already exists in the data directory
    """
    movies_df = pd.read_csv(DATA_PATH + "/ml-100k/u.item", sep='|', header=None, encoding="ISO-8859-1")
    movies_df.columns = ["movieId", "movie title", "release date", "video release date", "IMDb URL ", "unknown", "Action",
                     "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
                     "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
    movies_df = movies_df.set_index('movieId')

    ratings_df = pd.read_csv(DATA_PATH + '/ml-100k/u.data', sep='\t', header=None)
    ratings_df.columns = ['userId', 'movieId', 'rating', 'timestamp']
    ratings_df = ratings_df.astype({'userId': str, 'movieId': str})
    ratings_df.rating = ratings_df.rating / ratings_df.rating.max()
    ratings_df = ratings_df.set_index(['userId', 'movieId'])

    user_df = pd.read_csv(DATA_PATH + '/ml-100k/u.user', sep='|', header=None, encoding="ISO-8859-1")
    user_df.columns = ['userId', 'age', 'gender', 'occupation', 'zip']
    user_df = user_df.set_index('userId')

    return movies_df, ratings_df, user_df


def split_by_months(movies_df, ratings_df, users_df):
    """
    return a list of dataframes,
    movies/ratings/users for each month
    """

    year_month_tuples = get_months_list(ratings_df)

    movies_df_dict = dict({})
    ratings_df_dict = dict({})
    users_df_dict = dict({})

    for time_tuple in year_month_tuples:
        ratings_df_temp = ratings_df.loc[timestamp_matches_month(ratings_df, time_tuple)]

        # get user/item dfs with only users/items
        # that actually show up in the data
        user_list = [int(x) for x in ratings_df_temp.index.get_level_values(0).unique()]
        item_list = [int(x) for x in ratings_df_temp.index.get_level_values(1).unique()]

        users_df_temp = users_df.loc[user_list]
        movies_df_temp = movies_df.loc[item_list]

        movies_df_dict[time_tuple] = movies_df_temp
        ratings_df_dict[time_tuple] = ratings_df_temp
        users_df_dict[time_tuple] = users_df_temp

    return movies_df_dict, ratings_df_dict, users_df_dict, year_month_tuples


def main():
    construct_movielens_predicates()


if __name__ == '__main__':
    main()
