import time
import os
import json
import fcntl
import csv
import sys
import logging
import os
import shutil
from common import broadcast_copies
from common import middleware
from common import utils
from common import routing

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['MAX_DAY_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['MAX_DAY_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['MAX_DAY_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['MAX_DAY_FILTER']['hashing_attributes'].split('|')
NODE_ID = config['MAX_DAY_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
TARGET_COLUMN = config['MAX_DAY_FILTER']['target_column']

CURRENT_STAGE_NAME = config['MAX_DAY_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['MAX_DAY_FILTER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['MAX_DAY_FILTER']['next_stage_amount'].split(',')
NEXT_STAGES_NAMES = config['MAX_DAY_FILTER']['next_stage_name'].split(',')

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGES_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)

class MaxDayFilter:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.max_date = {} # key: client_id, value: [None, 0]
        self.clients_dates_views = {} # key: client_id, value: (key: day, value: views sum)

    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        if input_message['case'] == 'eof':
            self.clients_received_eofs[client_id] += 1
            if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:
                input_message['date'] = self.max_date[client_id][0]
                input_message['view_count'] = self.max_date[client_id][1]
                del self.clients_received_eofs[client_id]
                del self.max_date[client_id]
                del self.clients_dates_views[client_id]
                return input_message
        else:
            return input_message
        return None


    def filter_max_date(self, input_message, client_id):
        client_dictionary = self.clients_dates_views[client_id]

        temp = time.strptime(input_message['trending_date'], '%Y-%m-%dT%H:%M:%SZ')
        trending_date = time.strftime('%Y-%m-%d',temp)
        amount_delta = int(input_message[TARGET_COLUMN])
        if not (trending_date in client_dictionary):
            client_dictionary[trending_date] = 0
        client_dictionary[trending_date] += amount_delta
        amount_new = client_dictionary[trending_date]

        if self.max_date[client_id][1] <= amount_new:
            self.max_date[client_id][0] = trending_date
            self.max_date[client_id][1] = amount_new 

    def process_received_message(self, input_message):
        client_id = input_message['client_id']
        message_to_send = None

        if not (client_id in self.clients_dates_views):
            self.clients_dates_views[client_id] = {}
            self.max_date[client_id] = [None, 0]
            self.clients_received_eofs[client_id] = 0

        if input_message['type'] == 'data':
            self.filter_max_date(input_message, client_id)
        else:
            message_to_send = self.process_control_message(input_message)

        if message_to_send != None:
            self.middleware.send(message_to_send)

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    wrapper = MaxDayFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()