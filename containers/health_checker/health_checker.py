import os
from election import leader_election

ELECTION_ID = int(os.environ['ELECTION_ID'])
ELECTION_REPLICAS_AMOUNT = int(os.environ['ELECTION_REPLICAS_AMOUNT'])

le = leader_election.LeaderElection(ELECTION_REPLICAS_AMOUNT, ELECTION_ID)
le.run()
