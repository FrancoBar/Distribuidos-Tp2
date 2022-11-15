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
HASHING_ATTRIBUTES = config['MAX_DAY_FILTER']['hashing_attributes'].split(',')
NODE_ID = config['MAX_DAY_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['MAX_DAY_FILTER']['port'])
FLOWS_AMOUNT = int(config['MAX_DAY_FILTER']['flows_amount'])

CURRENT_STAGE_NAME = config['MAX_DAY_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = config['MAX_DAY_FILTER']['previous_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT = config['MAX_DAY_FILTER']['next_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_NAME = config['MAX_DAY_FILTER']['next_stage_name'] # Hacer un for de las etapas anteriores

# aux_client_id = 'generic_client_id'

class MaxDayFilter:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()
        self.max_date = {} # key: client_id, value: [None, 0]
        self.clients_dates_views = {} # key: client_id, value: (key: day, value: views sum)

    def _on_recv_eof(self, input_message):
        client_id = input_message['client_id']
        output_message = None
        if self.max_date[client_id][0]:
            output_message = {'type':'data', 'date':self.max_date[client_id][0], 'view_count':self.max_date[client_id][1], 'client_id': client_id}
        del self.max_date[client_id]
        return output_message

    def _on_last_eof(self, input_message):
        # utils.clear_all_files(STORAGE)
        return {'type':'control', 'case':'eof'}

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
        return None


    def process_received_message(self, input_message):
        client_id = input_message['client_id']

        

        if not (client_id in self.clients_dates_views):
            self.clients_dates_views[client_id] = {}
            self.max_date[client_id] = [None, 0]
            self.clients_received_eofs[client_id] = 0

        if input_message['type'] == 'data':
            return self.filter_max_date(input_message, client_id)
        else:
            if input_message['case'] == 'eof':
                return broadcast_copies.broadcast_copies(self.middleware, input_message, ID, COPIES, self._on_recv_eof, self._on_last_eof)

            return None

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