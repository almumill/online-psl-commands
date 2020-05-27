from scripts import constants
import math

###
# create_command_line takes in all the parameters of your desired commmand
# and returns the formatted line of text representing the command
###

def create_command_line(command_type, pred_type, pred_name, pred_constants, value):
	command_line = ""

	if command_type == constants.ADD:
		command_str = "add"
	elif command_type == constants.UPDATE:
		command_str = "update"
	elif command_type == constants.DELETE:
		command_str = "delete"

	if pred_type == constants.OBS:
		type_str = "obs"
	elif pred_type == constants.TARGET:
		type_str = "target"
	elif pred_type == constants.TRUTH:
		type_str = "truth"

	name_str = pred_name

	constants_list = ""
	constant_count = len(pred_constants)
	for i in range(constant_count - 1):
		constants_list += str(int(pred_constants[i])) + " "
	constants_list += str(int(pred_constants[constant_count - 1]))

	value_str = str(value)

	return command_str + "\t" + type_str + "\t" + name_str + "\t" + constants_list + "\t" + value_str + "\n"

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
