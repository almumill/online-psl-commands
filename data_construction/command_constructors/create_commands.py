import sys
sys.path.insert(0, './command_constructors')
from command_utils import create_command_line


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


def df_to_command(constants_df, value_series, action_type, partition_name, predicate_name):
	command_list = []
	assert(constants_df.shape[0] == value_series.shape[0])
	row_count = constants_df.shape[0]
	col_count = constants_df.shape[1]
	for idx, row in constants_df.iterrows():
		constants_list = row.values
		if value_series.loc[idx].shape[0] != 0:
			value = value_series.loc[idx].values[0]
		else:
			value = None
		command_list += [create_command_line(action_type, partition_name, predicate_name, constants_list, value)]
	return command_list
