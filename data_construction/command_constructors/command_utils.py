import constants
import os
OUT_DIRECTORY = "../psl-datasets/movielens/data/movielens"
COMMAND_DIRECTORY_NAME = "commands"


def command_file_write(command_list, fold, setting, file_name):
	command_str = ''
	for command in command_list:
		command_str += command + '\n'

	path = os.path.join(OUT_DIRECTORY, fold, setting, COMMAND_DIRECTORY_NAME)
	if not os.path.exists(path):
		os.makedirs(path)

	print(path)

	with open(os.path.join(path, file_name), 'w') as writer:
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

