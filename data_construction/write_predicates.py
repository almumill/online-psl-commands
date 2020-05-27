import pandas as pd
import os

from helpers import get_months_list, get_month_and_date, timestamp_matches_month
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
        ratings_df = ratings_df_dict[ordered_list_of_months[i]]
        observed_ratings_df, truth_ratings_df = partition_by_timestamp(ratings_df)
        user_df = user_df_dict[ordered_list_of_months[i]]
        movies_df = movies_df_dict[ordered_list_of_months[i]]

        users = ratings_df.userId.unique()
        movies = ratings_df.movieId.unique()

        ratings_predicate(observed_ratings_df, truth_ratings_df, str(i))
        nmf_ratings_predicate(observed_ratings_df, truth_ratings_df, str(i))
        rated_predicate(observed_ratings_df, truth_ratings_df, str(i))
        item_predicate(observed_ratings_df, truth_ratings_df, str(i))
        user_predicate(observed_ratings_df, truth_ratings_df, str(i))
        target_predicate(truth_ratings_df, str(i))
        average_item_rating_predicate(observed_ratings_df, truth_ratings_df, str(i))
        average_user_rating_predicate(observed_ratings_df, truth_ratings_df, str(i))
        sim_content_predicate(movies_df, str(i))
        sim_items_predicate(observed_ratings_df, truth_ratings_df, movies, str(i))
        sim_users_predicate(observed_ratings_df, truth_ratings_df, users, str(i))
        print("Did split #"+str(i))
    for i in range(len(ordered_list_of_months)):
        print("Fold #"+str(i)+" -- " + str(ordered_list_of_months[i]))


def partition_by_timestamp(ratings_df, train_proportion=0.8):
    sorted_frame = ratings_df.sort_values(by='timestamp')
    return (sorted_frame.iloc[: int(sorted_frame.shape[0] * train_proportion), :],
            sorted_frame.iloc[int(sorted_frame.shape[0] * train_proportion):, :])


def filter_dataframes(movies_df, ratings_df, user_df):
    """
    Get rid of users who have not yet rated more than n movies
    Note that there are no users where there are less than 20 ratings occurring in the raw datatset
    """
    return movies_df, ratings_df.groupby('userId').filter(lambda x: x.shape[0] > 5), user_df


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
        user_list = [int(x) for x in ratings_df_temp['userId'].unique()]
        item_list = [int(x) for x in ratings_df_temp['movieId'].unique()]

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
