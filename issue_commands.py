import socket

def create_packet(command):

	do_delete = (len(command) == 4)

	packet_data = ""
	
	# command type
	packet_data += str(command[0]) + "\n"

	# predicate changed type
	packet_data += str(command[1]) + "\n"

	# predicate name
	packet_data += str(command[2]) + "\n"

	# constants
	for const in command[3]:
		packet_data += const + " "
	packet_data += "\n"
	
	# value to be updated, if applicable
	if not do_delete:
		packet_data += command[4] + "\n"
	
	return packet_data.encode('utf-8')

def issue_command(command, socket):
	packet = create_packet(command)
	socket.send(packet)
	return True
