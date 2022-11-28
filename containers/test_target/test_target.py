import socket
import os
import sys
import logging
from common import transmition_udp

logging.basicConfig(level=logging.INFO)

FAIL_AFTER = int(os.environ['FAIL_AFTER'])
SKIP_AFTER = int(os.environ['SKIP_AFTER'])

health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
health_socket.bind(('', 2114))

i = 0
try:
	while(True):
		if FAIL_AFTER > 0 and i >= FAIL_AFTER:
			sys.exit(1)

		n, address = transmition_udp.recv_uint32(health_socket)

		if SKIP_AFTER > 0 and i >= SKIP_AFTER:
			i = 0
		else:
			transmition_udp.send_uint32(health_socket, n, address)
			i = i + 1
except Exception as e:
	logging.exceptions(e)



