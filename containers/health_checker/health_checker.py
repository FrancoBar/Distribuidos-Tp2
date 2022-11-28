import os
import time
import logging
import multiprocessing
from common import health_check
from election import leader_election

health_check_process = multiprocessing.Process(target=health_check.echo_server, daemon=False)
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
