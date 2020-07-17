import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '..')
from helpers import query_relevance_cosine_similarity
from helpers import write

def cosine_sim_users_series(observed_ratings_df, users):
    user_cosine_similarity_series = query_relevance_cosine_similarity(
        observed_ratings_df.loc[:, ['rating']].reset_index(),
        'userId', 'movieId')

    # take top 50 for each user to define pairwise blocks
    user_cosine_similarity_block_frame = pd.DataFrame(index=users, columns=range(25))
    for u in observed_ratings_df.index.get_level_values(0).unique():
        user_cosine_similarity_block_frame.loc[u, :] = user_cosine_similarity_series.loc[u].nlargest(25).index

    # some users may not have rated any movie in common with another user
    user_cosine_similarity_block_frame = user_cosine_similarity_block_frame.dropna(axis=0)

    flattened_frame = user_cosine_similarity_block_frame.values.flatten()
    user_index = np.array([[i] * 25 for i in user_cosine_similarity_block_frame.index]).flatten()
    user_cosine_similarity_block_index = pd.MultiIndex.from_arrays([user_index, flattened_frame])
    user_cosine_similarity_block_series = pd.Series(data=1, index=user_cosine_similarity_block_index)

    return user_cosine_similarity_block_series

def sim_users_predicate(sim_users_series, time_step, fold='0', setting='eval'):
    """
    User Similarity Predicate: write the sim_users_series to predicate files
    """
    write(sim_users_series, 'sim_users_obs_ts_'+str(time_step), fold, setting)

