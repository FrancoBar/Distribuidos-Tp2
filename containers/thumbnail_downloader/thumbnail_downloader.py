import os
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

class ThumbnailsDownloader:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()

    def _on_last_eof(self, input_message):
        return {'type':'control', 'case':'eof'}

    def download_thumbnail(self, input_message):
        try:
            with urllib.request.urlopen(input_message['thumbnail_link']) as response:
                img_data = response.read()
                base64_data = base64.b64encode(img_data).decode('utf-8')
                return {'type':'data', 'case':'img', 'video_id':input_message['video_id'], 'img_data':base64_data}
        except Exception as e:
            logging.exception(e)
            middleware.stop()
        return None


    def process_received_message(self, input_message):

        if input_message['type'] == 'data':
            return self.download_thumbnail(input_message)
        else:
            if input_message['case'] == 'eof':
                return broadcast_copies.broadcast_copies(self.middleware, input_message, ID, COPIES, None, self._on_last_eof)
            
            return None

    def start_received_messages_processing(self):
        self.middleware.run()


def main():
    wrapper = ThumbnailsDownloader()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()