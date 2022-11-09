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

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['MAX_DAY_FILTER']['input_queue']
OUTPUT_QUEUE = config['MAX_DAY_FILTER']['output_queue']
OUTPUT_COLUMNS = config['MAX_DAY_FILTER']['output_columns'].split(',')
TARGET_COLUMN = config['MAX_DAY_FILTER']['target_column']
STORAGE = config['MAX_DAY_FILTER']['storage']

aux_client_id = 'generic_client_id'

class MaxDayFilter:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()
        self.max_date = {} # key: client_id, value: [None, 0]
        self.clients_dates_views = {} # key: client_id, value: (key: day, value: views sum)

    def _on_recv_eof(self, input_message):
        client_id = aux_client_id
        output_message = None
        if self.max_date[client_id][0]:
            output_message = {'type':'data', 'date':self.max_date[client_id][0], 'view_count':self.max_date[client_id][1], 'client_id': client_id}
        del self.max_date[client_id]
        return output_message

    def _on_last_eof(self, input_message):
        utils.clear_all_files(STORAGE)
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
        client_id = aux_client_id
        if not (client_id in self.clients_dates_views):
            self.clients_dates_views[client_id] = {}
            self.max_date[client_id] = [None, 0]
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