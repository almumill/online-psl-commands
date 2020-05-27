from scripts import constants


# take a line from the command file and return an object that issue_command can read

def parse_command(line):

	command = []
	args = line.split("\t")

	do_delete = False

	# process command type
	if args[0] == "add":
		command += [constants.ADD]
	elif args[0] == "update":
		command += [constants.UPDATE]
	elif args[0] == "delete":
		command += [constants.DELETE]
		do_delete = True

	# process target predicate type
	if args[1] == "target":
		command += [constants.TARGET]
	elif args[1] == "truth":
		command += [constants.TRUTH]
	elif args[1] == "obs":
		command += [constants.OBS]

	# process predicate name
	command += [args[2]]

	# process constants
	command += [args[3].split(" ")]

	# process value to update
	if not do_delete:
		command += [args[4]]

	return command
