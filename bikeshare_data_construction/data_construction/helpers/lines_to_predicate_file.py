def write_lines_to_predicate_file(filename, lines):
	print("herre")
	handle = open(filename, "w")
	for line in lines:
		handle.write(line)
		handle.write("\n")