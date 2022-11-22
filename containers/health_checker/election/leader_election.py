import sys
import socket
import errno
import time
import logging
import multiprocessing
import signal
from .dns import id_to_addr, addr_to_id
from common import transmition_udp

TIMEOUT_IDLE = 8
TIMEOUT_ELECTION = 2

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

class LeaderElection:

	def __init__(self, replicas_amount , id, work_callback):
		self.open = True
		self.id = id
		self.replicas_amount = replicas_amount
		self.work_callback = work_callback
		self.leader_id = replicas_amount - 1
		self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
		self.socket.bind(('', id_to_addr(id)[1]))
		self.state = self._state_election
		signal.signal(signal.SIGTERM, self.sigterm_handler_parent)

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
		logging.debug('From: {} Msg: {}'.format(other_id, DEBUG_MSG[msg]))
		return (other_id, msg)

	def _state_election(self):
		logging.debug("Election")
		self.broadcast(MSG_ELECTION)
		received_ack = False
		while self.open:
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
				logging.debug('TIMEOUT')
				if received_ack:
					self.state = self._state_idle
				else:
					self.state = self._state_leader
				break

	def _state_idle(self):
		logging.debug("Idle")
		while self.open:
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
				logging.debug('TIMEOUT')
				self.state = self._state_election
				break

	def _state_leader(self):
		logging.debug("Leader")
		self.broadcast(MSG_LEADER)
		working_process = multiprocessing.Process(target=self._working_process, daemon=False)
		working_process.start()
		try:
			while self.open:
				other_id, msg = self.recv(None)
				if msg == MSG_LEADER:
					self.leader_id = other_id
					self.state = self._state_idle
					break
				else:
					if msg == MSG_ELECTION and other_id < self.id:
						self.send(MSG_ACK, other_id)
					self.state = self._state_election
					break
		except socket.error:
			pass
		working_process.terminate()

	def _broadcast_process(self):
		signal.signal(signal.SIGTERM, self.sigterm_handler_child)
		while self.open:
			self.broadcast(MSG_ALIVE)
			time.sleep(.2)

	def _working_process(self):
		signal.signal(signal.SIGTERM, self.sigterm_handler_child)
		broadcast_process = multiprocessing.Process(target=self._broadcast_process, daemon=False)
		broadcast_process.start()
		self.work_callback()

	def run(self):
		try:
			while(self.open and self.state):
				self.state()
		except socket.error as e:
			pass

	def sigterm_handler_parent(self, signum, frame):
		logging.info('Sigterm received')
		self.open = False
		self.socket.close()

	def sigterm_handler_child(self, signum, frame):
		self.open = False
		for child in multiprocessing.active_children():
			child.terminate()
		sys.exit(0)