from command_constructors.command_utils import command_file_write, create_command_line
import command_constructors.constants as constants

def predicate_diff_to_commands(pred_file_1, pred_file_2, pred_name, partition):
    handle_1 = open(pred_file_1, "r")
    handle_2 = open(pred_file_2, "r")

    file_1_lines = handle_1.readlines()
    file_2_lines = handle_2.readlines()

    pred_file_1_dict = dict({})

    command_lines = []

    for line in file_1_lines:
        tokens = line.split("\t")
        constant_count = len(tokens) - 1
        const_list = [0] * constant_count
        for const_cursor in range(constant_count - 1):
            const_list[constant_count] = tokens[const_cursor]
        const_tuple = tuple(const_list)
        pred_file_1_dict[const_tuple] = tokens[constant_count]

    pred_file_2_dict = dict({})

    for line in file_2_lines:
        tokens = line.split("\t")
        constant_count = len(tokens) - 1
        const_list = [0] * constant_count
        for const_cursor in range(constant_count - 1):
            const_list[constant_count] = tokens[const_cursor]
        const_tuple = tuple(const_list)
        pred_file_2_dict[const_tuple] = tokens[constant_count]

    for const_tuple in pred_file_2_dict.keys():
        const_list = list(const_tuple)
        value = pred_file_2_dict[const_tuple]
        if const_tuple not in pred_file_1_dict.keys():
            command_lines += create_command_line(constants.ADD, constants.OBS, pred_name, const_list,
                                                 value)
        else:
            command_lines += create_command_line(constants.UPDATE, constants.OBS, pred_name, const_list,
                                                 value)

    for const_tuple in pred_file_1_dict.keys():
        const_list = list(const_tuple)
        if const_tuple not in pred_file_2_dict.keys():
            command_lines += create_command_line(constants.DELETE, constants.OBS, pred_name, const_list,
                                                 pred_file_2_dict[const_tuple])