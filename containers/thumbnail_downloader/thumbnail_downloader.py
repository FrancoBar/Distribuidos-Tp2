import os
import urllib.request
import base64
import logging
# from common import broadcast_copies
from common import middleware
from common import poisoned_middleware
from common import utils
from common import routing
from common import query_state
from common import general_filter

ID=os.environ['HOSTNAME']
# COPIES=int(os.environ['COPIES'])

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
PREVIOUS_STAGE_AMOUNT = int(config['THUMBNAIL_DOWNLOADER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['THUMBNAIL_DOWNLOADER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['THUMBNAIL_DOWNLOADER']['next_stage_name'].split(',')

IS_POISONED = os.environ['IS_POISONED'] == 'true'

def read_value(query, key, value):
    if key == 'eof':
        if not (key in query):
            query[key] = 0
        query[key] += 1
    elif key == 'config':
        query[key] = value
    else:
        raise Exception(f'Unexpected key in log: {key}')

def write_value(query, key, value):
    return str(value)

class ThumbnailsDownloader(general_filter.GeneralFilter):
    def __init__(self):
        middleware_instance = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.last_stage_router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        query_state_instance = query_state.QueryState('/root/storage/', read_value, write_value)
        super().__init__(NODE_ID, PREVIOUS_STAGE_AMOUNT, middleware_instance, query_state_instance)

    def _on_config(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        client_values['config'] = 'config'
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'config', 'config')
        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def _on_last_eof(self, input_message):
        client_id = input_message['client_id']
        input_message['msg_id'] = self.query_state.get_id(client_id)
        input_message['origin'] = self.node_id
        input_message['producer'] = 'img'
        self.middleware.send(input_message)
        self.query_state.delete_query(client_id)


    def process_data_message(self, input_message):
        client_id = input_message['client_id']
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'])
        try:
            with urllib.request.urlopen(input_message['thumbnail_link']) as response:
                img_data = response.read()
                base64_data = base64.b64encode(img_data).decode('utf-8')
                message_data = {'type':'data', 'producer':'img', 'video_id':input_message['video_id'], 'img_data':base64_data, 'client_id': input_message['client_id']}
                message_data['msg_id'] = self.query_state.get_id(client_id)
                message_data['origin'] = NODE_ID
                self.middleware.send(message_data)
        except Exception as e:
            logging.exception(e)
            middleware.stop()
        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def process_priority_message(self, input_message):
        client_id = input_message['client_id']
        if input_message['case'] == 'disconnect':
            self.query_state.delete_query(client_id)

    def start_received_messages_processing(self):
        self.middleware.run()


def main():
    wrapper = ThumbnailsDownloader()
    wrapper.start_received_messages_processing()
    wrapper.stop_health_process()

if __name__ == "__main__":
    main()