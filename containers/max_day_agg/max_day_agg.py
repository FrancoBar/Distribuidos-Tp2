import json
import time
from common import middleware
from common import utils

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['MAX_AGG_FILTER']['input_queue']
OUTPUT_QUEUE = config['MAX_AGG_FILTER']['output_queue']

max_date = [None, 0]

def filter_max_agg(input_message):
    global max_date
    amount_new = int(input_message['view_count'])
    if max_date[1] <= amount_new:
        max_date[0] = input_message['date']
        max_date[1] = amount_new
    return None

def message_callback(middleware, input_message):
    global max_date
    if input_message['type'] == 'data':
       return filter_max_agg(input_message)
    else:
        if input_message['case'] != 'eof':
            return None

        output_message = {'type':'data', 'case':'max_date', 'date':max_date[0], 'view_count':max_date[1]}
        middleware.send(output_message)
        max_date = [None, 0]

        return {'type':'control', 'case':'eof'}

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, message_callback)
middleware.run()