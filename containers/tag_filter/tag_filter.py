import time
import sys
import os
import multiprocessing
from common import middleware
from common import utils

config = utils.initialize_config()
RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['TAG_FILTER']['input_queue']
OUTPUT_QUEUE = config['TAG_FILTER']['output_queue']
OUTPUT_COLUMNS = config['TAG_FILTER']['output_columns'].split(',')
TARGET_TAG =  config['TAG_FILTER']['target_tag']


class TagFilter:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.previous_stage_size = self.middleware.get_previous_stage_size()


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