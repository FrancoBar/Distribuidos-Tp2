#!/usr/bin/env python3
import sys
import logging
from asyncio import IncompleteReadError
from client_handler import ClientHandler
from common import middleware
from common import utils
import multiprocessing as mp
from common import routing
from common import health_check
from server import Server

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

PORT = int(config['REQUEST_LISTENER']['port'])
CURRENT_STAGE_NAME = config['REQUEST_LISTENER']['current_stage_name']


def process_connection(process_id, accepted_socket, client_id):
    handler = ClientHandler()
    handler.handle_connection(process_id, accepted_socket, client_id)

class RequestListener:
    def __init__(self):
        self.health_check_process = mp.Process(target=health_check.echo_server, daemon=False)
        self.health_check_process.start()
        self.server = Server(PORT, 1, process_connection)

    def run(self):
        self.server.run()

    def stop_health_process(self):
        self.health_check_process.kill()
        self.health_check_process.join()

def main():
    wrapper = RequestListener()
    wrapper.run()
    wrapper.stop_health_process()

if __name__ == "__main__":
    main()
