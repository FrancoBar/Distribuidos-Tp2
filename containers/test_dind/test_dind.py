import logging
import subprocess
import os
import sys
import time
import logging
import os
import socket
from common import transmition_udp

time.sleep(2)

logging.basicConfig(level=logging.DEBUG)

SERVICES = os.environ['SERVICES'].split(',')
TIMEOUT = 3
SLEEP_SECONDS = 1
RETRIES = 3

logging.info(SERVICES)

health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
health_socket.bind(('', 2114))
health_socket.settimeout(TIMEOUT)

i = 0
while True:
	for service in SERVICES:
		for retries in range(RETRIES):
			try:
				i = i + 1
				transmition_udp.send_uint32(health_socket, i, (service, 2114))
				while True:
					n, address = transmition_udp.recv_uint32(health_socket)
					if i == n:
						logging.debug('{}, {}, {}'.format(n, service, address))
						break
				break
			except (socket.gaierror, socket.timeout):
				if retries >= RETRIES - 1:
					result = subprocess.run(['docker', 'restart', service], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
					logging.debug('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))
				else:
					logging.debug('Retry sending to ' + service)
	time.sleep(SLEEP_SECONDS)

