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

def filter_tag(middleware, input_message):
    if input_message['type'] == 'control':
        return input_message

    if TARGET_TAG in input_message['tags']: 
        return {k: input_message[k] for k in OUTPUT_COLUMNS}
    
    return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, filter_tag)
middleware.run()
