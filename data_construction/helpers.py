import pandas as pd
import numpy as np
from datetime import datetime

from sklearn.metrics import pairwise_distances

PSL_DATASET_PATH = '../psl-datasets/movielens/data/movielens/'


def write(frame, predicate_name, fold, setting):
    frame.to_csv(PSL_DATASET_PATH + fold + '/' + setting + '/' + predicate_name + '.txt',
                 sep='\t', header=False, index=True)


def query_relevance_cosine_similarity(relevance_df, query_index, item_index, fill=True):
    """
    Builds query similarity predicate from a ratings data frame.
    Note: In this implementation we are considering the union of relevance values between queries, so if the
    relevance score is missing for one query, it is assumed to be 0 and considered in similarity calculation.
    We may want to first find the intersection of existing relevance items, then use those to calculate similarity.
    :param relevance_df: A dataframe with a query, item and relevance column fields
    :param query_index: name of query field
    :param item_index: name of item field
    :param fill: whether to fill missing entries with 0s, if false then we find the cosine similarity of only the overlapping ratings
    :return: multi index (query_id, item_id) Series
    """
    query_relevance_frame = relevance_df.set_index([query_index, item_index]).unstack()

    query_cosine_similarity_frame = pd.DataFrame(cosine_similarity_frame_from_relevance(query_relevance_frame, fill),
                                                 index=query_relevance_frame.index, columns=query_relevance_frame.index)

    return query_cosine_similarity_frame.stack()


def cosine_similarity_frame_from_relevance(data_frame, fill=True):
    if fill is True:
        return pairwise_distances(data_frame, metric=cosine_similarity_from_relevance_arrays,
                                  force_all_finite='allow-nan')
    else:
        return pairwise_distances(data_frame.fillna(0), metric=cosine_similarity_from_relevance_arrays,
                                  force_all_finite='allow-nan')


def cosine_similarity_from_relevance_arrays(x, y):
    overlapping_dot_product = (x * y)
    overlapping_indices = ~np.isnan(overlapping_dot_product)
    if overlapping_indices.sum() == 0:
        return 0
    else:
        return (overlapping_dot_product[overlapping_indices].sum() /
                (np.linalg.norm(x[overlapping_indices]) * np.linalg.norm(y[overlapping_indices])))


def get_month_and_date(timestamp):
    """
    Return (year, month) of given timestamp
    """
    ttuple = datetime.fromtimestamp(timestamp).timetuple()
    return (ttuple[0], ttuple[1])


def get_months_list(ratings_df):
    """
    Get a list of (year, month) tuples that exist
    in the ratings dataset, sorted by year and then
    month
    """

    timestamps = ratings_df['timestamp'].values
    months_histogram = dict({})

    for ts in timestamps:
        timestamp_tuple = get_month_and_date(ts)
        try:
            months_histogram[timestamp_tuple] += 1
        except:
            months_histogram[timestamp_tuple] = 1

    # sort by year and then month
    tuple_list = sorted(list(months_histogram.keys()), key=lambda x: x[0])

    # get sorted list of unique years
    set_of_years = set()
    for tuple in tuple_list:
        set_of_years.add(tuple[0])

    list_of_years = sorted(set_of_years)
    tuples_per_year = dict({})

    # within each year, sort by month and construct a final list
    final_list = []
    for year in list_of_years:
        final_list += sorted([tuple for tuple in tuple_list if tuple[0] == year], key=lambda x: x[1])

    return final_list


def timestamp_matches_month(ratings_df, time_tuple):
    """
    return a boolean array where True indicates
    that the corresponding row of ratings_df has a
    timestamp from the same year/month as time_tuple
    """
    bool_array = []
    for row in ratings_df.iterrows():
         if get_month_and_date(row[1]['timestamp']) == time_tuple:
             bool_array += [True]
         else:
             bool_array += [False]
    return bool_array


def standardize_ratings(observed_ratings_df, truth_ratings_df):
    # obs
    observed_ratings_series = observed_ratings_df.loc[:, ['userId', 'movieId', 'rating']].set_index(
        ['userId', 'movieId'])

    observed_by_user = observed_ratings_series.groupby(level=0)
    user_means = observed_by_user.mean()
    user_std = observed_by_user.std().fillna(0)

    mean_of_means = user_means.mean()
    mean_of_stds = user_std.mean()


    for user in observed_ratings_series.index.get_level_values(0).unique():
        if user_std.loc[user].values[0] != 0.0:
            observed_ratings_series.loc[user, :] = ((observed_ratings_series.loc[user, :] - user_means.loc[user].values[0])
                                                 / (4 * user_std.loc[user].values[0])) + 0.5
        else:
            observed_ratings_series.loc[user, :] = ((observed_ratings_series.loc[user, :] - mean_of_means)
                                                 / (4 * mean_of_stds)) + 0.5

    observed_ratings_series = observed_ratings_series.clip(lower=0, upper=1)

    # truth

    truth_ratings_series = truth_ratings_df.loc[:, ['userId', 'movieId', 'rating']].set_index(['userId', 'movieId'])

    for user in truth_ratings_series.index.get_level_values(0).unique():
        try:
            truth_ratings_series.loc[user, :] = ((truth_ratings_series.loc[user, :] - user_means.loc[user].values[0])
                                                     / (4 * user_std.loc[user].values[0])) + 0.5
        except KeyError as e:
            truth_ratings_series.loc[user, :] = ((truth_ratings_series.loc[user, :] - mean_of_means)
                                                 / (4 * mean_of_stds)) + 0.5

    truth_ratings_series = truth_ratings_series.clip(lower=0, upper=1)

    return observed_ratings_series, truth_ratings_series
