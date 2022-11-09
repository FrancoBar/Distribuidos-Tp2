import logging
import socket
import os
import logging
from common import middleware
from common import utils

PORT = int(os.environ['SERVER_PORT'])
utils.initialize_log("INFO")

logging.info("Tcpex up")

def filter_func(input_message):
    #logging.info(input_message)
    return input_message

def route_key_gen(message):
    if message['type'] == 'control':
        return 'control'
    return message['id']

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('', PORT))
server_socket.listen(1)
accepted_socket, addr = server_socket.accept()

middleware = middleware.TCPExchangeFilter("rabbitmq", accepted_socket, "output_ex", route_key_gen, filter_func)
middleware.run()