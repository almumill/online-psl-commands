import sys
from parse_commands import parse_command
from issue_commands import issue_command
import constants
import socket

# process command line arguments

command_file_name = sys.argv[1]
host_addr = sys.argv[2]
port = int(sys.argv[3])

# create connection with server

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host_addr, port))

if command_file_name == "-":
	file_handle = sys.stdin
else:
	file_handle = open(command_file_name, "r")

lines = file_handle.readlines()

for line in lines:
	command = parse_command(line)
	issue_command(command, s)
