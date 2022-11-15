import time
import sys
import os
import multiprocessing
from common import middleware
from common import utils
from common import routing

config = utils.initialize_config()
RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['TAG_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['TAG_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['TAG_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['TAG_FILTER']['hashing_attributes'].split(',')
NODE_ID = config['TAG_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['TAG_FILTER']['port'])
FLOWS_AMOUNT = int(config['TAG_FILTER']['flows_amount'])
TARGET_TAG =  config['TAG_FILTER']['target_tag']

CURRENT_STAGE_NAME = config['TAG_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = config['TAG_FILTER']['previous_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT = config['TAG_FILTER']['next_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_NAME = config['TAG_FILTER']['next_stage_name'] # Hacer un for de las etapas anteriores


class TagFilter:
    def __init__(self):
        # self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                            CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()


    def filter_tag(self, input_message):
        if TARGET_TAG in input_message['tags']:
            return {k: input_message[k] for k in OUTPUT_COLUMNS}
        return None

    def process_received_message(self, input_message):
        if input_message['type'] == 'control':
            return input_message
        # If not eof
        return self.filter_tag(input_message)

        # If client eof
            # volar todo y enviar

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = TagFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()