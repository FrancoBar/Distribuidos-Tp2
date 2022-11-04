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

max_date = [None, 0]

def _filter_max_date(file, input_message):
    global max_date
    fcntl.flock(file, fcntl.LOCK_EX)

    amount_previous = 0
    read_buffer = file.read()
    if read_buffer and read_buffer != '':
        amount_previous = int(read_buffer)
    amount_delta = int(input_message[TARGET_COLUMN])
    amount_new = amount_previous + amount_delta

    file.seek(0)
    file.write('{}'.format(amount_new))
    file.flush()

    fcntl.flock(file, fcntl.LOCK_UN)
    
    if max_date[1] <= amount_new:
        max_date[0] = input_message['trending_date']
        max_date[1] = amount_new 

def filter_max_date(middleware, input_message):
    temp = time.strptime(input_message['trending_date'], '%Y-%m-%dT%H:%M:%SZ')
    input_message['trending_date'] = time.strftime('%Y-%m-%d',temp)

    file_path = STORAGE + input_message['trending_date']
    try:
        #Opens the file as in r+ mode but creates it if it doesn't exists
        file_fd = os.open(file_path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC)
        with  os.fdopen(file_fd, "r+") as date_file:
            _filter_max_date(date_file, input_message)
    except Exception as e:
        logging.exception(e)
        middleware.stop()
    finally:
        return None

def _on_recv_eof(middleware, input_message):
    global max_date
    output_message = None
    if max_date[0]:
        output_message = {'type':'data', 'date':max_date[0], 'view_count':max_date[1]}
    max_date = [None, 0]
    return output_message

def _on_last_eof(middleware, input_message):
    utils.clear_all_files(STORAGE)
    return {'type':'control', 'case':'eof'}

def message_callback(middleware, input_message):
    if input_message['type'] == 'data':
       return filter_max_date(middleware, input_message)
    else:
        if input_message['case'] == 'eof':
            return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, _on_recv_eof, _on_last_eof)

        return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, message_callback)
middleware.run()
