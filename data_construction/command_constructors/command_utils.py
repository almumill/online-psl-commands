import constants
import math
import os
OUT_DIRECTORY = "../../psl-datasets/movielens/data/movielens"

###
# create_command_line takes in all the parameters of your desired commmand
# and returns the formatted line of text representing the command
###


def command_file_write(command_list, fold, setting, file_name):
	command_str = ''
	for command in command_list:
		command_str += command + '\n'

	path = os.path.join(OUT_DIRECTORY, fold, setting, file_name)
	with open(path, 'w') as writer:
		writer.write(command_str)


def create_command_line(command_type, partition_name, pred_name, pred_constants, value):
	if partition_name == constants.OBS:
		partition_str = "READ"
	elif partition_name == constants.TARGET:
		partition_str = "WRITE"
		
	constants_list = ""
	constant_count = len(pred_constants)
	for i in range(constant_count - 1):
		constants_list += str(int(pred_constants[i])) + "\t"
	constants_list += str(int(pred_constants[constant_count - 1]))

	value_str = str(value)

	if command_type == constants.ADD:
		command_str = "AddAtom"
		
		if value is not None:
			return command_str + "\t" + partition_str + "\t" + pred_name + "\t" + constants_list + "\t" + value_str
		else:
			return command_str + "\t" + partition_str + "\t" + pred_name + "\t" + constants_list
		
	elif command_type == constants.UPDATE:
		command_str = "UpdateObservation"
		return command_str + "\t" + pred_name + "\t" + constants_list + "\t" + value_str
		
	elif command_type == constants.DELETE:
		command_str = "Delete"
		return command_str + "\t" + partition_str + "\t" + pred_name + "\t" + constants_list

###
# create_proportional_slices takes in the length of what you want to slice,
# a list of proportion sizes for the slices (must sum to 1)
# and an optional base to add to each endpoint of these slices
#
# calling them slices is technically wrong, it returns ranges
# and will likely be modified to return lists of indices that
# aren't sequential in the future
###

def create_proportional_slices(total_len, proportion_list, base = 0):
	slices = []
	i = 0
	end = 0
	proportion_sum = 0
	while i < len(proportion_list):
		if i == 0:
			begin = 0
		else:
			begin = end
		proportion_sum += proportion_list[i]
		end = math.floor(total_len * proportion_sum)
		slices += [range(begin + base, end + base)]
		i += 1
	return slices
