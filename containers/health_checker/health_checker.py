import os
import time
import logging
import multiprocessing as mp
from common import utils
from common import health_check
from election import leader_election

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

health_check_process = mp.Process(target=health_check.echo_server, daemon=True)
health_check_process.start()

logging.basicConfig(level=logging.DEBUG)

ELECTION_ID = int(os.environ['ELECTION_ID'])
ELECTION_REPLICAS_AMOUNT = int(os.environ['ELECTION_REPLICAS_AMOUNT'])
SERVICES = os.environ['SERVICES'].split(',')
RETRIES = int(os.environ['RETRIES'])

def dummy():
	health_check.monitor(SERVICES, RETRIES)

le = leader_election.LeaderElection(ELECTION_REPLICAS_AMOUNT, ELECTION_ID, dummy)
le.run()

health_check_process.kill()
health_check_process.join()
