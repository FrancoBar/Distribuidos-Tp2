import time
import os
import json
import fcntl
import csv
import sys
import logging
import os
import shutil
from common import middleware
from common import query_state
from common import general_filter
from common import utils
from common import routing

ID=os.environ['HOSTNAME']
# COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['MAX_DAY_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['MAX_DAY_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['MAX_DAY_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['MAX_DAY_FILTER']['hashing_attributes'].split('|')
NODE_ID = config['MAX_DAY_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
TARGET_COLUMN = config['MAX_DAY_FILTER']['target_column']

CURRENT_STAGE_NAME = config['MAX_DAY_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['MAX_DAY_FILTER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['MAX_DAY_FILTER']['next_stage_amount'].split(',')
NEXT_STAGES_NAMES = config['MAX_DAY_FILTER']['next_stage_name'].split(',')

# routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGES_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)
routing_function = routing.generate_routing_function(None, NEXT_STAGES_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)

def update_client_max_date(client_values, trending_date, amount_delta):
    if not ('data' in client_values):
        client_values['data'] = {'dates_views': {}, 'max_date': [None, 0]}
    if not (trending_date in client_values['data']['dates_views']):
        client_values['data']['dates_views'][trending_date] = 0
    client_values['data']['dates_views'][trending_date] += amount_delta
    amount_new = client_values['data']['dates_views'][trending_date]

    if client_values['data']['max_date'][1] <= amount_new:
        client_values['data']['max_date'][0] = trending_date
        client_values['data']['max_date'][1] = amount_new


def read_value(query, key, value):
    if key == 'data':
        stored_values = value.split(',')
        trending_date = stored_values[0]
        amount_delta = stored_values[1]
        update_client_max_date(query, trending_date, int(amount_delta))
    elif key == 'eof':
        if not (key in query):
            query[key] = 0
        query[key] += 1
    elif key == 'config':
        query[key] = value
    else:
        raise Exception(f'Unexpected key in log: {key}')

def write_value(query, key, value):
    return str(value)

class MaxDayFilter(general_filter.GeneralFilter):
    def __init__(self):
        middleware_instance = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        query_state_instance = query_state.QueryState('/root/storage/', read_value, write_value)
        super().__init__(NODE_ID, PREVIOUS_STAGE_AMOUNT, middleware_instance, query_state_instance)

    def _on_last_eof(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        input_message['msg_id'] = self.query_state.get_id(client_id)
        input_message['origin'] = NODE_ID
        input_message['date'] = client_values['data']['max_date'][0]
        input_message['view_count'] = client_values['data']['max_date'][1]
        self.middleware.send(input_message)
        self.query_state.delete_query(client_id)

    def process_data_message(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)

        temp = time.strptime(input_message['trending_date'], '%Y-%m-%dT%H:%M:%SZ')
        trending_date = time.strftime('%Y-%m-%d',temp)
        amount_delta = int(input_message[TARGET_COLUMN])

        update_client_max_date(client_values, trending_date, amount_delta)

        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'data', f'{trending_date},{amount_delta}')
        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    wrapper = MaxDayFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()