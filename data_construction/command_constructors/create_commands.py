import sys
sys.path.insert(0, './command_constructors')
from command_utils import create_command_line
import command_constructors.constants as constants

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

# convert ints to tuple form for
# indexing into seen_atoms dictionaries
def to_tuple(constants):
    if type(constants) == int:
        return (constants, )
    else:
        return constants

# convert 1-tuples into raw ints
# for indexing into series
def to_atom_constants(constants_tuple):
    if len(constants_tuple) == 1:
        atom_constants = constants_tuple[0]
    else:
        atom_constants = constants_tuple
    return atom_constants

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

"""
update_seen_atoms updates the dictionary used to keep track of encountered atoms.
If an atom in the given series has never appeared, it's added.
If an atom's value changes in the given series, it's updated.

If delete_unseen is true, every atom in seen_atoms that's not in the series
gets deleted. This is used when we need to delete things no longer observed in
the current timestep, like some sim_users/sim_items atoms.
"""

def update_seen_atoms(series, seen_atoms, delete_unseen = False):

    # update the dict of all observed atoms values
    for atom_constants in series.index:
        value = series.loc[atom_constants]

        # Convert single ints to tuple form
        # for the purposes of our seen_atoms dictionaries
        constants_tuple = to_tuple(atom_constants)

        if constants_tuple not in seen_atoms.keys() or seen_atoms[constants_tuple] != value:
            seen_atoms[constants_tuple] = value

    # delete any atoms from our dictionary which didn't show up in the current series
    # this gets used when updating the blocked user similarities
    if delete_unseen:
        to_delete = []

        for constants_tuple in seen_atoms.keys():
            # If we're dealing with 1-tuples, then we index into the series
            # using its integer value. 2-tuples+ index into the series
            # themselves.
            atom_constants = to_atom_constants(constants_tuple)

            # Delete all atoms not seen in the current series
            if atom_constants not in series.index:
                to_delete += [constants_tuple]

        # delete unseen atoms in this timestep
        for unseen_atom in to_delete:
            seen_atoms.pop(unseen_atom)

    return seen_atoms


"""
series_to_dynamic_commands takes in a series, indexed by PSL constants,
and issues add/updates as appropriate according to which atoms which have been
encountered previously, stored in the seen_atoms dict.

Set delete_unseen to true to delete all atoms from previous timesteps
which are no longer observed in the current one, such as when getting rid
of similarities that are no longer in the blocked set. (sim_users, sim_items)
"""

def series_to_dynamic_commands(series, seen_atoms, predicate_name, delete_unseen = False):
    command_list = []

    # issue ADD/UPDATE commands appropriately
    # for each entry in the series
    for atom_constants in series.index:
        value = series.loc[atom_constants]

        # convert ints to tuple form to index into
        # the seen_atoms dict
        constants_tuple = to_tuple(atom_constants)

        if constants_tuple not in seen_atoms.keys():
            command_list += [create_command_line(constants.ADD, constants.OBS, predicate_name, constants_tuple, value)]
        elif seen_atoms[constants_tuple] != value:
            command_list += [create_command_line(constants.UPDATE, constants.OBS, predicate_name, constants_tuple, value)]

    # delete any atoms that were observed in the last timestep but not the current one
    # such as blocked user/item similarities
    if delete_unseen:
        for constants_tuple in seen_atoms.keys():

            # If we're dealing with 1-tuples, then we index into the series
            # using its integer value. 2-tuples+ index into the series
            # themselves.
            atom_constants = to_atom_constants(constants_tuple)

            # Issue commands to delete everything that isn't in the current
            # series at this timestep
            if atom_constants not in series.index:
                command_list += [create_command_line(constants.DELETE, constants.OBS, predicate_name, constants_tuple, None)]

    return command_list