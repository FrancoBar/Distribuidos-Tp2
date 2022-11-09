import logging
import socket
import os
from common import middleware
from common import utils

HOST = os.environ['SERVER_HOST']
PORT = int(os.environ['SERVER_PORT'])
utils.initialize_log("INFO")
logging.info("Extcp up")

def filter_func(input_message):
    #logging.info(input_message)
    return input_message

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST, PORT))

middleware = middleware.ExchangeTCPFilter("rabbitmq", "extcp_ex", "default", "control", client_socket, filter_func)
middleware.run()