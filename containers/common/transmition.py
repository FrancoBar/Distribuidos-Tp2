from asyncio import IncompleteReadError
from .serialize import *
import socket
import json

def _recv_sized(socket, size):
	"""
	Receives exactly 'num_bytes' bytes through the provided socket.
	If no bytes are read from the socket IncompleteReadError is raised
	Source: https://stackoverflow.com/questions/55825905/how-can-i-reliably-read-exactly-n-bytes-from-a-tcp-socket
	"""
	buf = bytearray(size)
	pos = 0
	while pos < size:
		n = socket.recv_into(memoryview(buf)[pos:])
		if n == 0:
			raise IncompleteReadError(bytes(buf[:pos]), size)
		pos += n
	return bytes(buf)

def _recv_unsigned_number(socket, size):
	return deserialize_unsigned_number(_recv_sized(socket, size))

def recv_uint32(socket):
	return _recv_unsigned_number(socket, UINT32_SIZE)

def recv_uint64(socket):
	return _recv_unsigned_number(socket, UINT64_SIZE)

def recv_str(socket):
	size = recv_uint32(socket)
	return deserialize_str(_recv_sized(socket, size))

def send_str(socket, msg):
	size = serialize_uint32(len(msg))
	m = serialize_str(msg)
	socket.sendall(size + m)
	# aux = socket.sendall(size + m)
	# if msg == json.dumps('XXXXX'):
	# 	print(f'BORRAR return de send_all: {aux}')
	# # socket.send(size + m)

def send_uint32(socket, msg):
	m = serialize_uint32(msg)
	socket.sendall(m)

def send_bool(socket, msg):
	m = serialize_bool(msg)
	socket.sendall(m)