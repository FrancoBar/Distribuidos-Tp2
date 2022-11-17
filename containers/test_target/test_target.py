import socket
import os
import sys

FAIL_AFTER = int(os.environ['FAIL_AFTER'])

health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
health_socket.bind(('', 2114))

i = 0
while(True):
	byte_array, address = health_socket.recvfrom(1)
	health_socket.sendto(byte_array, address)
	i = i + 1
	if i >= FAIL_AFTER:
		sys.exit(1)
