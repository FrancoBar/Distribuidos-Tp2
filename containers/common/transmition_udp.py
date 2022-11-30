from asyncio import IncompleteReadError
from .serialize import *
import socket

def _recv_sized(socket, size):
	"""
	Receives exactly 'num_bytes' bytes through the provided socket.
	If no bytes are read from the socket IncompleteReadError is raised
	Source: https://stackoverflow.com/questions/55825905/how-can-i-reliably-read-exactly-n-bytes-from-a-tcp-socket
	"""
	buf = bytearray(size)
	prev_address = None
	pos = 0
	while pos < size:
		n, address = socket.recvfrom_into(memoryview(buf)[pos:], size)
		if n == 0:
			raise IncompleteReadError(bytes(buf[:pos]), size)
		# if prev_address and prev_address != address:
		# 	raise IncompleteReadError(bytes(buf[:pos]), size)
		prev_address = address
		pos += n
	return bytes(buf), prev_address

def _recv_unsigned_number(socket, size):
	bytes_buffer, address = _recv_sized(socket, size)
	return deserialize_unsigned_number(bytes_buffer), address

def recv_uint32(socket):
	return _recv_unsigned_number(socket, UINT32_SIZE)

def recv_str(socket):
	size, size_address = recv_uint32(socket)
	string, string_address = _recv_sized(socket, size)
	if size_address != string_address:
		raise IncompleteReadError(size)
	return string, size_address

def send_str(socket, msg, address):
	size = serialize_uint32(len(msg))
	m = serialize_str(msg)
	socket.sendto(size + m, address)

def send_uint32(socket, msg, address):
	m = serialize_uint32(msg)
	socket.sendto(m, address)
