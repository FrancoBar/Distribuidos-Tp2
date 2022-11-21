import os
import time
import logging
from election import leader_election

logging.basicConfig(level=logging.INFO)

ELECTION_ID = int(os.environ['ELECTION_ID'])
ELECTION_REPLICAS_AMOUNT = int(os.environ['ELECTION_REPLICAS_AMOUNT'])

def health_checking_callback():
	while True:
		logging.info('Hola')
		time.sleep(1)

le = leader_election.LeaderElection(ELECTION_REPLICAS_AMOUNT, ELECTION_ID, health_checking_callback)
le.run()
