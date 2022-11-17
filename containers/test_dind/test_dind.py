import logging
import subprocess
import os
import sys
import time
import logging
import os
import socket

SERVICES = os.environ['SERVICES'].split(',')
SLEEP_SECONDS = 1

health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
health_socket.bind(('', 2114))
health_socket.settimeout(3)

time.sleep(2)

logging.basicConfig(level=logging.INFO)
logging.info(SERVICES)
while True:
	for service in SERVICES:
		try:
			health_socket.sendto(b'A', (service, 2114))
			#Check address == IMAGE
			byte_array, address = health_socket.recvfrom(1)
			logging.info(service + ', ' + str(address) + ', ' + str(byte_array))
		except (socket.gaierror, socket.timeout):
			logging.error(service)
			subprocess.run(['docker', 'restart', service], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			#logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))
		
	time.sleep(SLEEP_SECONDS)