import sys
import socket
import errno
import time
import multiprocessing

ID = int(sys.argv[1])
BASE_PORT = 4321

REPLICAS_AMOUNT = 3
TIMEOUT_IDLE = 2
TIMEOUT_ELECTION = 5

MSG_ELECTION=0
MSG_LEADER=1
MSG_ACK=2
MSG_ALIVE=3

DEBUG_MSG = {
	MSG_ACK : "ack",
	MSG_ELECTION : "elec",
	MSG_LEADER : "leader",
	MSG_ALIVE : "alive"
}

def serialize_uint8(u):
	return u.to_bytes(1, 'big')

def deserialize_uint(b):
	return int.from_bytes(b, byteorder='big', signed=False)

def id_to_addr(id):
	return ("localhost", BASE_PORT + id)

def addr_to_id(addr):
	return int(addr[1]) - BASE_PORT

class LeaderElection:

	def __init__(self, id):
		self.id = id
		self.leader_id = REPLICAS_AMOUNT - 1
		self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
		self.socket.bind(id_to_addr(id))
		self.state = self._state_election

	def broadcast(self, msg):
		other_id = 0
		while other_id < REPLICAS_AMOUNT:
			if other_id != self.id:
				self.send(msg, other_id)
			other_id = other_id + 1

	def send(self, msg, other_id):
		self.socket.sendto(serialize_uint8(msg), id_to_addr(other_id))

	def recv(self, timeout):
		self.socket.settimeout(timeout)
		byte_array, address = self.socket.recvfrom(2)
		other_id = addr_to_id(address)
		msg = deserialize_uint(byte_array)
		print("From", other_id, "Msg", DEBUG_MSG[msg])
		return (other_id, msg)

	def _state_election(self):
		print("Election")
		self.broadcast(MSG_ELECTION)
		received_ack = False
		while True:
			try:
				other_id, msg = self.recv(TIMEOUT_ELECTION)
				if msg == MSG_LEADER:
					self.leader_id = other_id
					self.state = self._state_idle
					break
				elif msg == MSG_ACK:
					received_ack = True
				elif msg == MSG_ELECTION:
					if other_id < self.id:
						self.send(MSG_ACK, other_id)
					else:
						received_ack = True
				else:
					pass
			except socket.timeout:
				print('TIMEOUT')
				if received_ack:
					self.state = self._state_idle
				else:
					self.state = self._state_leader
				break

	def _state_idle(self):
		print("Idle")
		while True:
			try:
				other_id, msg = self.recv(TIMEOUT_IDLE)
				if msg == MSG_LEADER:
					self.leader_id = other_id
				elif msg == MSG_ELECTION:
					if other_id < self.id:
						self.send(MSG_ACK, other_id)
					self.state = self._state_election
				else:
					pass
			except socket.timeout:
				print('TIMEOUT')
				self.state = self._state_election
				break

	def _state_leader(self):
		print("Leader")
		self.broadcast(MSG_LEADER)
		working_process = multiprocessing.Process(target=self._working_process)
		working_process.start()
		while True:
			other_id, msg = self.recv(None)
			if msg == MSG_LEADER:
				self.leader_id = other_id
				self.state = self._state_idle
			else:
				if msg == MSG_ELECTION and other_id < self.id:
					self.send(MSG_ACK, other_id)
				self.state = self._state_election
			working_process.kill()
			break

	def _working_process(self):
		while True:
			self.broadcast(MSG_ALIVE)
			time.sleep(.2)

	def run(self):
		while(self.state):
			self.state()

leader_election = LeaderElection(ID)
leader_election.run()
