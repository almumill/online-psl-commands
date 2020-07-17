import pandas as pd
import sys
sys.path.insert(0, '..')
from helpers import write

def average_user_rating_series(observed_ratings_df):
    return observed_ratings_df.reset_index()[["userId", "rating"]].groupby(
        "userId").mean().squeeze()

def average_user_rating_predicate(user_avg_rating_series, time_step, fold='0', setting='eval'):
    """
    Rated Predicates
    """
    write(user_avg_rating_series, 'avg_user_rating_obs_ts_'+str(time_step), fold, setting)