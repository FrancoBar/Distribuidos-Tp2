#!/usr/bin/env python3
import sys
import logging
from asyncio import IncompleteReadError
from common import middleware
from common import utils
from common import server
from common import routing

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['REQUEST_LISTENER']['input_exchange']
OUTPUT_EXCHANGE = config['REQUEST_LISTENER']['output_exchange']
OUTPUT_COLUMNS = config['REQUEST_LISTENER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['REQUEST_LISTENER']['hashing_attributes'].split(',')
HASHING_ATTRIBUTES_2 = config['REQUEST_LISTENER']['hashing_attributes_2'].split(',')
NODE_ID = config['REQUEST_LISTENER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['REQUEST_LISTENER']['port'])
FLOWS_AMOUNT = int(config['REQUEST_LISTENER']['flows_amount'])

PREVIOUS_STAGE_AMOUNT = "asdasdas" # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT = config['REQUEST_LISTENER']['next_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT_2 = config['REQUEST_LISTENER']['next_stage_amount_2'] # Hacer un for de las etapas anteriores

NEXT_STAGE_NAME = config['REQUEST_LISTENER']['next_stage_name'] # Hacer un for de las etapas anteriores
NEXT_STAGE_NAME_2 = config['REQUEST_LISTENER']['next_stage_name_2'] # Hacer un for de las etapas anteriores


aux_client_id = 'generic_client_id'

class RequestListener:
    def __init__(self):
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()
        self.entry_input = None
        self.entry_ouput = None
        self.server = server.Server(PORT, 1, self.connection_handler)

        self.aux_counter = 0

    def connection_handler(self, accept_socket):
        try:
            # self.entry_input = middleware.TCPChannelFilter(RABBIT_HOST, accept_socket, OUTPUT_QUEUE, self.entry_recv_callback)
            # self.entry_ouput = middleware.ChannelTCPFilter(RABBIT_HOST, INPUT_QUEUE, accept_socket, self.answers_callback)
            # def __init__(self, middleware_host, socket, output_exchange, output_route_key_gen, filter_func):
            self.entry_input = middleware.TCPExchangeFilter(RABBIT_HOST, accept_socket, OUTPUT_EXCHANGE, routing.router_two_receivers, self.entry_recv_callback)
            # def __init__(self, middleware_host, input_exchange, input_route_key, control_route_key, socket, filter_func):
            self.entry_ouput = middleware.ExchangeTCPFilter(RABBIT_HOST, INPUT_EXCHANGE, NODE_ID, CONTROL_ROUTE_KEY, accept_socket, self.answers_callback)
            
            logging.info('Receiving entries')
            self.entry_input.run()

            logging.info('Answering entries')
            self.entry_ouput.run()

        except IncompleteReadError as e:
            logging.error('Client abruptly disconnected')
            logging.exception(e)
        except Exception as e:
            raise e

    def entry_recv_callback(self, input_message):
        if input_message['type'] == 'control' and input_message['case'] == 'eof':
            self.entry_input.stop()
        return input_message

    def answers_callback(self, input_message):
        client_id = aux_client_id
        if input_message['type'] == 'control':
            if input_message['case'] == 'eof':
                if not (client_id in self.clients_received_eofs):
                    self.clients_received_eofs[client_id] = 0
                self.clients_received_eofs[client_id] += 1
                self.aux_counter += 1
                if self.clients_received_eofs[client_id] != FLOWS_AMOUNT:
                    return None
                else:
                    del self.clients_received_eofs[client_id]
            else:
                return None
        logging.warning(f"BORRAR received message {input_message}")
        return input_message

    def run(self):
        self.server.run()

def main():
    wrapper = RequestListener()
    wrapper.run()

if __name__ == "__main__":
    main()
