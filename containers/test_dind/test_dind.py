import logging
import subprocess
import os
import sys
import time
import logging
import os

IMAGE= os.environ['IMAGE']

SLEEP_SECONDS = 10
if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	logging.info(IMAGE)
	while True:
		result = subprocess.run(['docker', 'ps'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

		result = subprocess.run(['docker', 'stop', IMAGE], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

		result = subprocess.run(['docker', 'ps'], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

		result = subprocess.run(['docker', 'start', IMAGE], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))

		time.sleep(SLEEP_SECONDS)