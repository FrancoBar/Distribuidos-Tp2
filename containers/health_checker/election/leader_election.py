import sys
import socket
import errno
import time
import logging
import multiprocessing
from .dns import id_to_addr, addr_to_id
from common import transmition_udp

TIMEOUT_IDLE = 10
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

logging.basicConfig(level=logging.DEBUG)

class LeaderElection:

	def __init__(self, replicas_amount , id):
		self.id = id
		self.replicas_amount = replicas_amount
		self.leader_id = replicas_amount - 1
		self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
		self.socket.bind(('', id_to_addr(id)[1]))
		self.state = self._state_election

	def broadcast(self, msg):
		other_id = 0
		while other_id < self.replicas_amount:
			if other_id != self.id:
				self.send(msg, other_id)
			other_id = other_id + 1

	def send(self, msg, other_id):
		try:
			transmition_udp.send_uint32(self.socket, msg, id_to_addr(other_id))
		except socket.gaierror as e:
			pass

	def recv(self, timeout):
		self.socket.settimeout(timeout)
		msg, address = transmition_udp.recv_uint32(self.socket)
		other_id = addr_to_id(address)
		logging.info('From: {} Msg: {}'.format(other_id, DEBUG_MSG[msg]))
		return (other_id, msg)

	def _state_election(self):
		logging.info("Election")
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
				logging.info('TIMEOUT')
				if received_ack:
					self.state = self._state_idle
				else:
					self.state = self._state_leader
				break

	def _state_idle(self):
		logging.info("Idle")
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
				logging.info('TIMEOUT')
				self.state = self._state_election
				break

	def _state_leader(self):
		logging.info("Leader")
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
