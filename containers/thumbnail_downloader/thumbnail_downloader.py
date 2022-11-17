import os
import urllib.request
import base64
import logging
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
INPUT_EXCHANGE = config['THUMBNAIL_DOWNLOADER']['input_exchange']
OUTPUT_EXCHANGE = config['THUMBNAIL_DOWNLOADER']['output_exchange']
HASHING_ATTRIBUTES = config['THUMBNAIL_DOWNLOADER']['hashing_attributes'].split('|')
NODE_ID = config['THUMBNAIL_DOWNLOADER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']

CURRENT_STAGE_NAME = config['THUMBNAIL_DOWNLOADER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = config['THUMBNAIL_DOWNLOADER']['previous_stage_amount']
NEXT_STAGE_AMOUNTS = config['THUMBNAIL_DOWNLOADER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['THUMBNAIL_DOWNLOADER']['next_stage_name'].split(',')

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)


class ThumbnailsDownloader:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received

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

    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        if input_message['case'] == 'eof':
            self.clients_received_eofs[client_id] += 1
            if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:
                del self.clients_received_eofs[client_id]
                return input_message
        return None

    def process_received_message(self, input_message):
        client_id = input_message['client_id']
        message_to_send = None

        # Initialization
        if not (client_id in self.clients_received_eofs):
            self.clients_received_eofs[client_id] = 0

        # Message processing       
        if input_message['type'] == 'data':
            message_to_send = self.download_thumbnail(input_message)
        else:
            message_to_send = self.process_control_message(input_message)

        # Message sending
        if message_to_send != None:
            self.middleware.send(message_to_send)

    def start_received_messages_processing(self):
        self.middleware.run()


def main():
    wrapper = ThumbnailsDownloader()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()