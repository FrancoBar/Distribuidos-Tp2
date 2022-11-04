import time
import os
import fcntl
import urllib.request
import base64
import logging
from common import broadcast_copies
from common import middleware
from common import utils

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['THUMBNAIL_DOWNLOADER']['input_queue']
OUTPUT_QUEUE = config['THUMBNAIL_DOWNLOADER']['output_queue']

def download_thumbnail(middleware, input_message):
    try:
        with urllib.request.urlopen(input_message['thumbnail_link']) as response:
            img_data = response.read()
            base64_data = base64.b64encode(img_data).decode('utf-8')
            return {'type':'data', 'case':'img', 'video_id':input_message['video_id'], 'img_data':base64_data}
    except Exception as e:
        logging.exception(e)
        middleware.stop()
    return None


def _on_last_eof(middleware, input_message):
    return {'type':'control', 'case':'eof'}

def message_callback(middleware, input_message):
    if input_message['type'] == 'data':
       return download_thumbnail(middleware, input_message)
    else:
        if input_message['case'] == 'eof':
            return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, None, _on_last_eof)
        
        return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, message_callback)
middleware.run()
