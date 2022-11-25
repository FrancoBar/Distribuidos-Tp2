#!/usr/bin/env python3
import sys
import logging
from asyncio import IncompleteReadError
from common import middleware
from common import utils
from common import server
from common import routing
from client_handler import ClientHandler

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

PORT = int(config['REQUEST_LISTENER']['port'])
CURRENT_STAGE_NAME = config['REQUEST_LISTENER']['current_stage_name']

aux_client_id = 'generic_client_id'

def process_connection(accepted_socket, client_id):
    handler = ClientHandler()
    handler.handle_connection(accepted_socket, client_id)

class RequestListener:
    def __init__(self):
        self.server = server.Server(PORT, 1, process_connection)

    def run(self):
        self.server.run()

def main():
    wrapper = RequestListener()
    wrapper.run()

if __name__ == "__main__":
    main()
