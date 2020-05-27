import pandas as pd
from scripts import constants
import os
import numpy as np
from scripts.timestamps import get_months_list, timestamp_matches_month
from scripts.command_utils import create_command_line, create_proportional_slices
from scripts.helpers import query_relevance_cosine_similarity


# location of ratings data relative to CWD
DATA_FILE = "ml-100k/u.data"

# proprtional size of observed segment in a given split
OBS_PROPORTION = 0.7

# number of updates after observed segment
UPDATE_COUNT = 6

# proportional size of observed segment within each update
UPDATE_OBS_PROPORTION = 0.5

# the predicate we're updating in all cases here is rating
RATING_PREDICATE_NAME = "rating"

# user/item average predicate names
AVG_USR_RATING_PREDICATE_NAME = "avg_user_rating"
AVG_ITEM_RATING_PREDICATE_NAME = "avg_item_rating"

# user/item similarity predicates
USR_SIMILARITY_PREDICATE_NAME = "sim_users"
ITEM_SIMILARITY_PREDICATE_NAME = "sim_items"

# names of the columns which hold the constant values for
# the ratings atom
RATING_CONSTANT_COL_NAMES = ['userId', 'movieId']

# name of the column holding the value of the
# ratings atom
RATING_VALUE_COL_NAME = 'rating'

#################
# create_obs_update_proportions returns list of proportions representing segment
# sizes where the first segment is for our observed data and there are
# update_count remaining segments of equal size
#################

def create_obs_update_proportions(obs_proportion, update_count):
	proportions = []
	proportions += [obs_proportion]
	for i in range(update_count):
		proportions += [float('%.4f'%((1 - obs_proportion) / update_count))]
	return proportions

#################
# create_obs_target_proportions
# when given the proportional size of the observed segment
# within an update, returns a list with this size and
# the proportional size of the remaining data which are
# going to be targets
#################

def create_obs_target_proportions(update_obs_proportion):
	return [update_obs_proportion, 1 - update_obs_proportion]

##################
# get_ratings_df(filename) reads the data in filename
# and returns it as a dataframe sorted chronologically
##################

def get_ratings_df(filename):
	ratings_df = pd.read_csv(filename, sep='\t', header=None)
	ratings_df.columns = ['userId', 'movieId', 'rating', 'timestamp']
	ratings_df = ratings_df.sort_values(by=['timestamp'])
	ratings_df.rating = ratings_df.rating / ratings_df.rating.max()
	return ratings_df

#################
# split_df_by_month takes in a list of (year, month) tuples
# and returns a dictionary of dataframes
# containing only reviews from each month.
# 
# The key is the (year, month) tuple corresponding
# to the separated dataframe.
#
# ratings_df		= the total ratings dataframe to be split up
#
# months		= list of (year, month) tuples to be separated by
#################

def split_df_by_month(ratings_df, months):
	split_dfs_dict = dict({})
	for month in months:
		split_dfs_dict[month] = ratings_df.loc[timestamp_matches_month(ratings_df, month)]
	return split_dfs_dict


#################
# row_to_atom(row) takes in a single-row Series object representing an atom's
# constants and its value, a list of the column names corresponding the constants,
# and the name of the column containing the value and returns a list of constants
# and the predicate value separately
#################

def row_to_atom(row, constant_cols, value_col):
	constant_list = []
	for col in constant_cols:
		constant_list += [row[col]]

	value = row[value_col]

	return constant_list, value


#################
# join_df_slices(split, all_observed_segments) takes a dataframe and a list of
# slices into the dataframe and returns a sub-dataframe sliced by
# these slices
#################
def join_df_slices(split, all_observed_segments):
	df_list = []
	for segment in all_observed_segments:
		df_list += [split.iloc[segment]]
	return pd.concat(df_list)

#################
# get_avg_values(df, group_col, value_col) returns the average
# value of value_col for all rows with a matching group_col value.
# 
# We use this to calculate user/movie rating averages
#################
def get_avg_values(df, group_col, value_col):
	averages = dict({})

	# index by the column we're grouping by
	df = df.reset_index()
	df = df.set_index(group_col)

	# calculate the mean within each group
	for uniqueId in df.index.unique():
		df_temp = df[df.index == uniqueId]
		averages[uniqueId] = df_temp[value_col].mean()

	return averages

#################
# get_user_similarities takes in a dataframe of observations and
# returns a dict where keys are user-user pairs and the values
# are their cosine similarities
#################

def get_user_similarities(observed_ratings_df):
	users = observed_ratings_df.userId.unique()
	user_cosine_similarity_series = query_relevance_cosine_similarity(
		observed_ratings_df.loc[:, ['userId', 'movieId', 'rating']],
		'userId', 'movieId')

	# take top 50 for each user to define pairwise blocks
	user_cosine_similarity_block_frame = pd.DataFrame(index=users, columns=range(25))
	for u in observed_ratings_df.userId.unique():
		user_cosine_similarity_block_frame.loc[u, :] = user_cosine_similarity_series.loc[u].nlargest(25).index

	# some users may not have rated any movie in common with another user
	user_cosine_similarity_block_frame = user_cosine_similarity_block_frame.dropna(axis=0)

	flattened_frame = user_cosine_similarity_block_frame.values.flatten()
	user_index = np.array([[i] * 25 for i in user_cosine_similarity_block_frame.index]).flatten()
	user_cosine_similarity_block_index = pd.MultiIndex.from_arrays([user_index, flattened_frame])
	user_cosine_similarity_block_series = pd.Series(data=1, index=user_cosine_similarity_block_index)

	similarities_dict = dict({})

	for index, value in user_cosine_similarity_block_series.items():
		similarities_dict[index] = value

	return similarities_dict

#################
# get_user_similarities takes in a dataframe of observations and
# returns a dict where keys are user-user pairs and the values
# are their cosine similarities
#################

def get_item_similarities(observed_ratings_df):
	movies = observed_ratings_df.movieId.unique()
	item_cosine_similarity_series = query_relevance_cosine_similarity(
	observed_ratings_df.loc[:, ['userId', 'movieId', 'rating']],
	'movieId', 'userId')

	# take top 25 for each movie to define pairwise blocks
	item_cosine_similarity_block_frame = pd.DataFrame(index=movies, columns=range(25))
	for m in observed_ratings_df.movieId.unique():
		item_cosine_similarity_block_frame.loc[m, :] = item_cosine_similarity_series.loc[m].nlargest(25).index

	# some movies may not have been rated by any user
	item_cosine_similarity_block_frame = item_cosine_similarity_block_frame.dropna(axis=0)
	flattened_frame = item_cosine_similarity_block_frame.values.flatten()
	item_index = np.array([[i] * 25 for i in item_cosine_similarity_block_frame.index]).flatten()
	item_cosine_similarity_block_index = pd.MultiIndex.from_arrays([item_index, flattened_frame])
	item_cosine_similarity_block_series = pd.Series(data=1, index=item_cosine_similarity_block_index)
	similarities_dict = dict({})

	for index, value in item_cosine_similarity_block_series.items():
		similarities_dict[index] = value

	return similarities_dict

#################
# create_command_files takes in a dataframe, partitions it,
# and creates files containing the appropriate commands
#
# split 			= the dataframe to be processed
#
# split_number			= the number of the split (1 is the first split, etc.)
#				* only used for naming files
#
# obs_update_proportions	= the proportions of data to be treated as initial observations
#				  and as updates
#				* follows form of [observed_proportion, update_proportion, update_proportion, etc...]
#
# obs_target_proportion		= the proportions of observations and targets in each update
#				* follows form of [observed_proportion, target_proportion]
#
# cut_method			= the method used to split up each update (default/random/etc.)
#################

def create_command_files(split, split_number, obs_update_proportions, update_obs_target_proportions, update_cut_method = "default"):

	##############################################
	## begin by processing initial observations ##
	##############################################

	# create the fold directory if it doesn't exist already
	dirname = "fold" + str(split_number) + "/"
	if not os.path.exists(dirname):
		os.makedirs(dirname)

	# sort this new dataframe chronologically
	split = split.sort_values(by=['timestamp'])

	# create the segments for the initial observations
	# and each update
	segments = create_proportional_slices(split.shape[0], obs_update_proportions)
	observation_segment = segments[0]
	update_segments = segments[1:]

	# keep track of (user, movie) pairs we've already encountered
	# in case somebody updates their rating
	seen_ratings = set({})

	# keep track of seen items/users for average updating purposes
	seen_users = set({})
	seen_items = set({})

	# bookkeeping to record the initial observations
	filename = "fold" + str(split_number) + "-init.txt"
	handle = open(dirname + filename, "w")
	file_buffer = ""

	# handle raw observation partition at the beginning
	for row_idx in observation_segment:
		row = split.iloc[row_idx]

		# update seen users for the purposes of average updatesa
		seen_users.add(row['userId'])
		seen_items.add(row['movieId'])

		# process atom constants/value
		predicate_constants, value = row_to_atom(row, RATING_CONSTANT_COL_NAMES, RATING_VALUE_COL_NAME)
		if tuple(predicate_constants) not in seen_ratings:
			file_buffer += create_command_line(constants.ADD, constants.OBS, RATING_PREDICATE_NAME, predicate_constants, value)
			seen_ratings.add(tuple(predicate_constants))
		else:
			file_buffer += create_command_line(constants.UPDATE, constants.OBS, RATING_PREDICATE_NAME, predicate_constants, value)

	# calculate user/item averages
	obs_df = split.iloc[observation_segment]
	user_rating_averages_dict = get_avg_values(obs_df, 'userId', 'rating')
	item_rating_averages_dict = get_avg_values(obs_df, 'movieId', 'rating')

	# add initial user average atoms
	for (user, avg) in user_rating_averages_dict.items():
		file_buffer += create_command_line(constants.ADD, constants.OBS, AVG_USR_RATING_PREDICATE_NAME, [user], avg)

	# add initial item average atoms
	for (item, avg) in item_rating_averages_dict.items():
		file_buffer += create_command_line(constants.ADD, constants.OBS, AVG_ITEM_RATING_PREDICATE_NAME, [item], avg)

	# calculate user/item collaborative filtering similarities
	user_similarities_dict = get_user_similarities(obs_df)
	item_similarities_dict = get_item_similarities(obs_df)

	for (user_pair, similarity) in user_similarities_dict.items():
		user_1 = user_pair[0]
		user_2 = user_pair[1]
		file_buffer += create_command_line(constants.ADD, constants.OBS, USR_SIMILARITY_PREDICATE_NAME, [user_1, user_2], similarity)

	for (item_pair, similarity) in item_similarities_dict.items():
		item_1 = item_pair[0]
		item_2 = item_pair[1]
		file_buffer += create_command_line(constants.ADD, constants.OBS, ITEM_SIMILARITY_PREDICATE_NAME, [item_1, item_2], similarity)

	# finish writing the initial observations file
	handle.write(file_buffer)

	# update the previous user/item pairs list with ones that are observed in the initial segment
	prev_seen_user_pairs = [x[0] for x in user_similarities_dict.items()]
	prev_seen_item_pairs = [x[0] for x in item_similarities_dict.items()]

	##########################################
	## begin processing the update segments ##
	##########################################

	# update the base we add to all endpoints of the segments
	# that we create in the future for each update
	base = len(observation_segment)

	# keep a list of all observed segments for calculating averages
	all_observed_segments = [observation_segment]

	# all remaining segments are updates, so we should handle them accordlingly
	for idx, temp_segment in enumerate(update_segments):
		# sequentially name the update files
		filename = "fold" + str(split_number) + "-update-" + str(idx) + ".txt"
		handle = open(dirname + filename, "w")
		file_buffer = ""

		# we cut the data into the obs and target splits
		if update_cut_method == "default":
			obs_target_slices = create_proportional_slices(len(temp_segment), update_obs_target_proportions, base)

		# the first segment is observed, second is targets
		obs_segment = obs_target_slices[0]
		target_segment = obs_target_slices[1]

		# keep track of the new observations for calculating averages
		all_observed_segments += [obs_segment]

		# handle the observation portion
		for row_idx in obs_segment:
			row = split.iloc[row_idx]
			predicate_constants, value = row_to_atom(row, RATING_CONSTANT_COL_NAMES, RATING_VALUE_COL_NAME)
			if tuple(predicate_constants) not in seen_ratings:
				file_buffer += create_command_line(constants.ADD, constants.OBS, RATING_PREDICATE_NAME, predicate_constants, value)
				seen_ratings.add(tuple(predicate_constants))
			else:
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, RATING_PREDICATE_NAME, predicate_constants, value)

		# update averages
		all_obs_df = join_df_slices(split, all_observed_segments)
		user_rating_averages_dict = get_avg_values(all_obs_df, 'userId', RATING_PREDICATE_NAME)
		item_rating_averages_dict = get_avg_values(all_obs_df, 'movieId', RATING_PREDICATE_NAME)

		# update user average predicates
		for (user, avg) in user_rating_averages_dict.items():
			if user not in seen_users:
				file_buffer += create_command_line(constants.ADD, constants.OBS, AVG_USR_RATING_PREDICATE_NAME, [user], avg)
				seen_users.add(user)
			else:
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, AVG_USR_RATING_PREDICATE_NAME, [user], avg)

		# update item average predicates
		for (item, avg) in item_rating_averages_dict.items():
			if item not in seen_items:
				file_buffer += create_command_line(constants.ADD, constants.OBS, AVG_ITEM_RATING_PREDICATE_NAME, [item], avg)
				seen_items.add(item)
			else:
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, AVG_ITEM_RATING_PREDICATE_NAME, [item], avg)

		# update collaborative filtering user/item similarities

		# calculate user/item collaborative filtering similarities
		user_similarities_dict = get_user_similarities(all_obs_df)
		item_similarities_dict = get_item_similarities(all_obs_df)


		new_user_pairs = [x[0] for x in user_similarities_dict.items()]
		new_item_pairs = [x[0] for x in item_similarities_dict.items()]

		# add/update user similarities
		for (user_pair, similarity) in user_similarities_dict.items():
			user_1 = user_pair[0]
			user_2 = user_pair[1]
			if user_pair not in prev_seen_user_pairs:
				file_buffer += create_command_line(constants.ADD, constants.OBS, USR_SIMILARITY_PREDICATE_NAME, [user_1, user_2], similarity)
			else:
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, USR_SIMILARITY_PREDICATE_NAME, [user_1, user_2], similarity)

		# add/update item similarities
		for (item_pair, similarity) in item_similarities_dict.items():
			item_1 = item_pair[0]
			item_2 = item_pair[1]
			if item_pair not in prev_seen_item_pairs:
				file_buffer += create_command_line(constants.ADD, constants.OBS, ITEM_SIMILARITY_PREDICATE_NAME, [item_1, item_2], similarity)
			else:
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, ITEM_SIMILARITY_PREDICATE_NAME, [item_1, item_2], similarity)

		# set any user/item similarities from the previous time step
		# which don't show up in the current one to have values of 0
		for user_pair in prev_seen_user_pairs:
			if user_pair not in new_user_pairs:
				user_1 = user_pair[0]
				user_2 = user_pair[1]
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, USR_SIMILARITY_PREDICATE_NAME, [user_1, user_2], 0)

		for item_pair in prev_seen_item_pairs:
			if item_pair not in new_item_pairs:
				item_1 = item_pair[0]
				item_2 = item_pair[1]
				file_buffer += create_command_line(constants.UPDATE, constants.OBS, ITEM_SIMILARITY_PREDICATE_NAME, [item_1, item_2], 0)

		# handle the targets/truth portion.userId.unique()
		# we create a command for both the target and truth predicates that need to be added when
		# a value is unobserved
		for row_idx in target_segment:
			row = split.iloc[row_idx]
			predicate_constants, value = row_to_atom(row, RATING_CONSTANT_COL_NAMES, RATING_VALUE_COL_NAME)
			if tuple(predicate_constants) not in seen_ratings:
				file_buffer += create_command_line(constants.ADD, constants.TARGET, RATING_PREDICATE_NAME, predicate_constants, value)
				file_buffer += create_command_line(constants.ADD, constants.TRUTH, RATING_PREDICATE_NAME, predicate_constants, value)
				seen_ratings.add(tuple(predicate_constants))
			else:
				file_buffer += create_command_line(constants.UPDATE, constants.TARGET, RATING_PREDICATE_NAME, predicate_constants, value)
				file_buffer += create_command_line(constants.UPDATE, constants.TRUTH, RATING_PREDICATE_NAME, predicate_constants, value)

		# finish up processing the segment
		handle.write(file_buffer)

		# update the previously-considered user/item similarities
		# to reflect what's observed in this time step
		prev_seen_user_pairs = new_user_pairs
		prev_seen_item_pairs = new_item_pairs

		# update the base so that future segments have the correct indices
		base += len(obs_segment) + len(target_segment)


def main():

	# process the raw data
	ratings_df = get_ratings_df(DATA_FILE)

	# get list of (year, month) tuples that appear in the data
	months = get_months_list(ratings_df)

	# split the reviews by month
	split_dfs_dict = split_df_by_month(ratings_df, months)

	# create the obs-update proportion list to segment the entire dataframe
	# and the obs-target proportions for the individual update segments
	obs_update_proportions = create_obs_update_proportions(OBS_PROPORTION, UPDATE_COUNT)
	update_obs_target_proportions = create_obs_target_proportions(UPDATE_OBS_PROPORTION)

	# process each split
	for idx, month in enumerate(months):
		split = split_dfs_dict[month]

		# actually create the command files
		create_command_files(split, idx, obs_update_proportions, update_obs_target_proportions, update_cut_method = "default")

if __name__ == '__main__':
    main()
