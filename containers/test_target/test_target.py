import socket
import os
import sys

FAIL_AFTER = int(os.environ['FAIL_AFTER'])
SKIP_AFTER = int(os.environ['SKIP_AFTER'])

health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
health_socket.bind(('', 2114))

i = 0
while(True):
	if FAIL_AFTER > 0 and i >= FAIL_AFTER:
		sys.exit(1)

	byte_array, address = health_socket.recvfrom(1)

	if SKIP_AFTER > 0 and i >= SKIP_AFTER:
		i = 0
	else:
		health_socket.sendto(byte_array, address)
		i = i + 1
