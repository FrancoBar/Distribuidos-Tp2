import json
import time
from common import middleware
from common import utils
from common import routing
from common import query_state
from common import general_filter

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['MAX_AGG_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['MAX_AGG_FILTER']['output_exchange']
HASHING_ATTRIBUTES = config['MAX_AGG_FILTER']['hashing_attributes'].split('|')
NODE_ID = config['MAX_AGG_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']

CURRENT_STAGE_NAME = config['MAX_AGG_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['MAX_AGG_FILTER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['MAX_AGG_FILTER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['MAX_AGG_FILTER']['next_stage_name'].split(',')

def read_value(query, key, value):
    if key == 'eof':
        if not (key in query):
            query[key] = 0
        query[key] += 1
        read_data_list = value.split(',')
        read_data_list[1] = int(read_data_list[1])
        query['data'] = read_data_list
    elif key == 'config':
        query[key] = value
    else:
        raise Exception(f'Unexpected key in log: {key}')

def write_value(query, key, value):
    day = value[0]
    views = value[1]
    return f'{day},{views}'


class MaxDayAggregator(general_filter.GeneralFilter):
    def __init__(self):
        middleware_instance = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.last_stage_router, self.process_received_message)
        query_state_instance = query_state.QueryState('/root/storage/', read_value, write_value)
        super().__init__(NODE_ID, PREVIOUS_STAGE_AMOUNT, middleware_instance, query_state_instance)

    def _on_config(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        client_values['config'] = 'config'
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'config', 'config')
        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def _on_eof(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        amount_new = int(input_message['view_count'])
        if not ('data' in client_values):
            client_values['data'] = [None, 0]
        if client_values['data'][1] <= amount_new:
            client_values['data'][0] = input_message['date']
            client_values['data'][1] = amount_new
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'eof', client_values['data'])
    # We take advantage of general_filter internal commit management when receiving an eof

    def _on_last_eof(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        output_message = {'type':'control', 'case': 'eof', 'producer':'max_date', 'client_id': client_id, 'date': client_values['data'][0], 'view_count': client_values['data'][1]}
        output_message['msg_id'] = self.query_state.get_id(client_id)
        output_message['origin'] = self.node_id
        self.middleware.send(output_message)
        self.query_state.delete_query(client_id)

    def process_data_message(self, input_message):
        raise Exception('Max day filter should only send data on eof: {input_message}')

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = MaxDayAggregator()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()