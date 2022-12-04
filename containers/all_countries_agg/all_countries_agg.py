import time
import os
import csv
import fcntl
from common import middleware
from common import poisoned_middleware
from common import utils
from common import routing
from common import query_state
from common import general_filter

INVALID_AMOUNT = -1

ID=os.environ['HOSTNAME']

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['ALL_COUNTRIES_AGG']['input_exchange']
OUTPUT_EXCHANGE = config['ALL_COUNTRIES_AGG']['output_exchange']
OUTPUT_COLUMNS = config['ALL_COUNTRIES_AGG']['output_columns'].split(',')
NODE_ID = config['ALL_COUNTRIES_AGG']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
MIN_DAYS = int(config['ALL_COUNTRIES_AGG']['min_days'])

CURRENT_STAGE_NAME = config['ALL_COUNTRIES_AGG']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['ALL_COUNTRIES_AGG']['previous_stage_amount'])
HASHING_ATTRIBUTES = config['ALL_COUNTRIES_AGG']['hashing_attributes'].split('|')
NEXT_STAGE_AMOUNTS = config['ALL_COUNTRIES_AGG']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['ALL_COUNTRIES_AGG']['next_stage_name'].split(',')

IS_POISONED = os.environ['IS_POISONED'] == 'true'

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)

def read_value(query, key, value):
    if key == 'data':
        if not (key in query):
            query[key] = {}
        value_array = value.split(',')
        video_id = value_array[0]
        used_date = value_array[1]
        country = value_array[2]
        if not (video_id in query[key]):
            query[key][video_id] = {}
        if not (used_date in query[key][video_id]):
            query[key][video_id][used_date] = set()
        query[key][video_id][used_date].add(country)
    elif key == 'eof':
        if not (key in query):
            query[key] = 0
        query[key] += 1
    elif key == 'config':
        query[key] = int(value)
    else:
        raise Exception(f'Unexpected key in log: {key}')

def write_value(query, key, value):
    return str(value)

class CountriesAmountFilter(general_filter.GeneralFilter):
    def __init__(self):
        if not IS_POISONED:
            middleware_instance = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                        CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        else:
            middleware_instance = poisoned_middleware.PoisonedExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                        CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        query_state_instance = query_state.QueryState('/root/storage/', read_value, write_value)
        super().__init__(NODE_ID, PREVIOUS_STAGE_AMOUNT, middleware_instance, query_state_instance)

    def _on_config(self, input_message):
        client_id = input_message['client_id']
        countries_amount = int(input_message['amount_countries'])
        self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'config', countries_amount)
        client_values = self.query_state.get_values(client_id)
        if not ('config' in client_values):
            client_values['config'] = countries_amount
            self.middleware.send(input_message)
        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def process_data_message(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)
        client_countries_amount = client_values['config']

        if not ('data' in client_values):
            client_values['data'] = {}

        temp=time.strptime(input_message['trending_date'], '%Y-%m-%dT%H:%M:%SZ')
        input_message['trending_date']=time.strftime('%Y-%m-%d',temp)
        used_date = input_message['trending_date']
        country = input_message['country']
        video_id = input_message['video_id']

        if not (video_id in client_values['data']):
            client_values['data'][video_id] = {}

        if not (used_date in client_values['data'][video_id]):
            client_values['data'][video_id][used_date] = set()

        if country in client_values['data'][video_id][used_date]:
            self.query_state.write(client_id, input_message['origin'], input_message['msg_id'])
        else:
            video_date_country = f'{video_id},{used_date},{country}'
            self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'data', video_date_country)
                
        client_values['data'][video_id][used_date].add(country)
        date_countries_amount = len(client_values['data'][video_id][used_date])

        all_countries_trending_days_amount = 0
        for date in client_values['data'][video_id]:
            if len(client_values['data'][video_id][date]) == client_countries_amount:
                all_countries_trending_days_amount += 1

        if (all_countries_trending_days_amount == MIN_DAYS) and (date_countries_amount == client_countries_amount):
            output_message = {k: input_message[k] for k in OUTPUT_COLUMNS}
            output_message['msg_id'] = self.query_state.get_id(client_id)
            output_message['origin'] = NODE_ID
            self.middleware.send(output_message)

        self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

def main():
    wrapper = CountriesAmountFilter()
    wrapper.start_received_messages_processing()
    wrapper.stop_health_process()

if __name__ == "__main__":
    main()