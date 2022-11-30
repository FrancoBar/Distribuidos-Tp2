import time
import sys
import os
import multiprocessing
from common import middleware
from common import query_state
from common import general_filter
from common import poisoned_middleware
from common import utils
from common import routing

config = utils.initialize_config()
RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['TAG_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['TAG_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['TAG_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['TAG_FILTER']['hashing_attributes'].split('|')
NODE_ID = config['TAG_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
TARGET_TAG =  config['TAG_FILTER']['target_tag']

CURRENT_STAGE_NAME = config['TAG_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['TAG_FILTER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['TAG_FILTER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['TAG_FILTER']['next_stage_name'].split(',')

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)

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

class TagFilter(general_filter.GeneralFilter):
# class TagFilter:
    def __init__(self):
        middleware_instance = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                            CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        # middleware_instance = poisoned_middleware.PoisonedExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
        #                                             CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        query_state_instance = query_state.QueryState('/root/storage/', read_value, write_value)
        super().__init__(NODE_ID, PREVIOUS_STAGE_AMOUNT, middleware_instance, query_state_instance)

    def process_data_message(self, input_message):
        client_id = input_message['client_id']
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'])
        if TARGET_TAG in input_message['tags']:
            message_to_send = {k: input_message[k] for k in OUTPUT_COLUMNS}
            message_to_send['msg_id'] = self.query_state.get_id(client_id)
            message_to_send['origin'] = NODE_ID
            self.middleware.send(message_to_send)
        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = TagFilter()
    wrapper.start_received_messages_processing()
    wrapper.stop_health_process()

if __name__ == "__main__":
    main()