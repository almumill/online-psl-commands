import pandas as pd
import sys
sys.path.insert(0, '..')

from helpers import write


def average_item_rating_series(observed_ratings_df):
    return observed_ratings_df.reset_index()[["movieId", "rating"]].groupby(
            "movieId").mean().squeeze()

def average_item_rating_predicate(item_avg_rating_series, time_step, fold='0', setting='eval'):
    """
    Rated Predicates
    """
    write(item_avg_rating_series, 'avg_item_rating_obs_ts_'+str(time_step), fold, setting)