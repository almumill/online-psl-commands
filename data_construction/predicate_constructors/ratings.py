import sys
sys.path.insert(0, '..')
from helpers import write


def ratings_predicate(observed_ratings_df, truth_ratings_df, fold='0', setting='eval'):
    """
    Ratings Predicates
    """

    # obs
    observed_ratings_series = observed_ratings_df.loc[:, ['userId', 'movieId', 'rating']].set_index(
        ['userId', 'movieId'])
    observed_by_user = observed_ratings_series.groupby(level=0)
    user_means = observed_by_user.mean()
    user_std = observed_by_user.std().fillna(1/4)
    for user in observed_ratings_series.index.get_level_values(0).unique():
        observed_ratings_series.loc[user, :] = ((observed_ratings_series.loc[user, :] - user_means.loc[user].values[0])
                                                 / (4 * user_std.loc[user].values[0])) + 0.5

    observed_ratings_series = observed_ratings_series.clip(lower=0, upper=1)

    write(observed_ratings_series, 'rating_obs', fold, setting)


    # truth

    truth_ratings_series = truth_ratings_df.loc[:, ['userId', 'movieId', 'rating']].set_index(['userId', 'movieId'])
    truth_ratings_series = truth_ratings_series.clip(lower=0, upper=1)

    write(truth_ratings_series, 'rating_truth', fold, setting)


    # target
    write(truth_ratings_series.loc[:, []], 'rating_targets', fold, setting)
