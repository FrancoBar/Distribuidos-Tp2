import time
import fcntl
import csv
import os
from common import broadcast_copies
from common import middleware
from common import utils

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['DUPLICATES_FILTER']['input_queue']
OUTPUT_QUEUE = config['DUPLICATES_FILTER']['output_queue']
OUTPUT_COLUMNS = config['DUPLICATES_FILTER']['output_columns'].split(',')
STORAGE = config['DUPLICATES_FILTER']['storage']
    
def filter_duplicates(input_message):
    if input_message['type'] == 'control':
        return input_message

    with open(STORAGE + input_message['video_id'], 'a+') as id_file:
        fcntl.flock(id_file, fcntl.LOCK_EX)
        id_file.seek(0)

        reader = csv.DictReader(id_file, fieldnames=['categoryId', 'title'], quotechar='"')
        for record in reader:
            if record['title'] == input_message['title'] and record['categoryId'] == input_message['categoryId']:
                fcntl.flock(id_file, fcntl.LOCK_UN)
                return None

        id_file.write('"{}","{}"\n'.format(input_message['categoryId'], input_message['title']))
        id_file.flush()
        fcntl.flock(id_file, fcntl.LOCK_UN)

    input_message['case']='unique_pair'
    return {k: input_message[k] for k in OUTPUT_COLUMNS}

def _on_last_eof(middleware, input_message):
    utils.clear_all_files(STORAGE)
    return {'type':'control', 'case':'eof'}

def message_callback(middleware, input_message):
    
    if input_message['type'] == 'data':
        return filter_duplicates(input_message)
    else:
        if input_message['case'] == 'eof':
            return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, None, _on_last_eof)

        return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, message_callback)
middleware.run()