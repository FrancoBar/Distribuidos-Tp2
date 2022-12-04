import logging
import subprocess
import time
import logging
from .utils import initialize_config, initialize_log
import socket
from .transmition_udp import send_uint32, recv_uint32

config = initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
initialize_log(LOGGING_LEVEL)


SLEEP_SECONDS = 1
TIMEOUT = 0.25
HEALTH_ECHO_PORT = 2114
HEALTH_MONITOR_PORT = 4114

def monitor(service_list, max_retries):
	try:
		health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
		health_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		health_socket.bind(('', HEALTH_MONITOR_PORT))
		health_socket.settimeout(TIMEOUT)
		i = 0

		while True:
			for service in service_list:
				for retries in range(max_retries):
					try:
						i = i + 1
						send_uint32(health_socket, i, (service, HEALTH_ECHO_PORT))
						while True:
							n, address = recv_uint32(health_socket)
							if i == n:
								logging.debug('{}, {}, {}'.format(n, service, address))
								break
							else:
								logging.debug('{} != {}'.format(i, n))
						break
					except (socket.gaierror, socket.timeout):
						if retries >= max_retries - 1:
							result = subprocess.run(['docker', 'restart', service], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
							logging.debug('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))
							logging.info('Restarting ' + service)
						else:
							logging.info('Retry sending to ' + service)
			time.sleep(SLEEP_SECONDS)
	except Exception as e:
		logging.exception(e)
	finally:
		health_socket.close()

def echo_server():
	health_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	health_socket.bind(('', HEALTH_ECHO_PORT))

	while True:
		try:
			n, address = recv_uint32(health_socket)
			send_uint32(health_socket, n, address)
		except Exception as e:
			logging.exception(e)
