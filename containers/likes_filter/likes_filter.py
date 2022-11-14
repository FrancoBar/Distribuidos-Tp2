import time
from common import middleware
from common import utils
from common import routing
import logging

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['LIKES_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['LIKES_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['LIKES_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['LIKES_FILTER']['hashing_attributes'].split(',')
NODE_ID = config['LIKES_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['LIKES_FILTER']['port'])
FLOWS_AMOUNT = int(config['LIKES_FILTER']['flows_amount'])
LIKES_MIN =  int(config['LIKES_FILTER']['min_likes'])

PREVIOUS_STAGE_AMOUNT = config['LIKES_FILTER']['previous_stage_amount']
NEXT_STAGE_AMOUNT = config['LIKES_FILTER']['next_stage_amount']
NEXT_STAGE_NAME = config['LIKES_FILTER']['next_stage_name']


class LikesFilter:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, f'LIKES_FILTER-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()


    def filter_likes(self, input_message):
        try:
            if int(input_message['likes']) >= LIKES_MIN:
                return {k: input_message[k] for k in OUTPUT_COLUMNS}
        except KeyError as e:
            logging.error('Entry lacks "likes" field.')
            logging.error(input_message)
        
        return None

    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        if input_message['case'] == 'eof':
            self.clients_received_eofs[client_id] += 1
            if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:
                del self.clients_received_eofs[client_id]
                return input_message
        else:
            return input_message
                
        return None

    def process_received_message(self, input_message):
        client_id = input_message['client_id']
        if not (client_id in self.clients_received_eofs):
            self.clients_received_eofs[client_id] = 0
        processing_result = None
        if input_message['type'] == 'control':
            processing_result = self.process_control_message(input_message)
        else:
            processing_result = self.filter_likes(input_message)

        if processing_result != None:
            self.middleware.send(processing_result)

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = LikesFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()