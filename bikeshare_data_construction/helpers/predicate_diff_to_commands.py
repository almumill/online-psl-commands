from command_constructors.command_utils import command_file_write, create_command_line
import command_constructors.constants as constants

def predicate_diff_to_commands(pred_file_1, pred_file_2, pred_name, partition):
    handle_1 = open(pred_file_1, "r")
    if pred_file_2:
        handle_2 = open(pred_file_2, "r")

    file_1_lines = handle_1.readlines()
    if pred_file_2:
        file_2_lines = handle_2.readlines()
    pred_file_1_dict = dict({})

    command_lines = []
    for line in file_1_lines:
        tokens = line.split("\t")

        if partition != constants.TARGET:
            constant_count = len(tokens) - 1
        else:
            constant_count = len(tokens)

        const_list = [0] * constant_count

        for const_cursor in range(constant_count):
            const_list[const_cursor] = tokens[const_cursor]

        if partition == constants.TARGET:
            const_list[constant_count-1] = const_list[constant_count-1].rstrip()

        const_tuple = tuple(const_list)

        if partition != constants.TARGET:
            pred_file_1_dict[const_tuple] = tokens[constant_count].rstrip()
        else:
            pred_file_1_dict[const_tuple] = None

    pred_file_2_dict = dict({})

    if pred_file_2:
        for line in file_2_lines:
            tokens = line.split("\t")

            if partition != constants.TARGET:
                constant_count = len(tokens) - 1
            else:
                constant_count = len(tokens)

            const_list = [0] * constant_count

            for const_cursor in range(constant_count):
                const_list[const_cursor] = tokens[const_cursor]

            if partition==constants.TARGET:
                const_list[constant_count - 1] = const_list[constant_count - 1].rstrip()


            const_tuple = tuple(const_list)

            if partition != constants.TARGET:
                pred_file_2_dict[const_tuple] = tokens[constant_count].rstrip()
            else:
                pred_file_2_dict[const_tuple] = None

    if pred_file_2:
        for const_tuple in pred_file_2_dict.keys():
            const_list = list(const_tuple)
            value = pred_file_2_dict[const_tuple]
            if const_tuple not in pred_file_1_dict.keys():
                command_lines += [create_command_line(constants.ADD, partition, pred_name, const_list,
                                                     value)]
            else:
                if pred_file_1_dict[const_tuple] != value:
                    command_lines += [create_command_line(constants.UPDATE, partition, pred_name, const_list,
                                                     value)]

        for const_tuple in pred_file_1_dict.keys():
            const_list = list(const_tuple)
            if const_tuple not in pred_file_2_dict.keys():
                command_lines += [create_command_line(constants.DELETE, partition, pred_name, const_list,
                                                     None)]

    else:
        for const_tuple in pred_file_1_dict.keys():
            value = pred_file_1_dict[const_tuple]
            const_list = list(const_tuple)
            command_lines += [create_command_line(constants.ADD, partition, pred_name, const_list,
                                                  value)]
    return command_lines