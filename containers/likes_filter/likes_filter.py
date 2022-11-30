import time
from common import middleware
from common import poisoned_middleware
from common import utils
from common import routing
from common import query_state
from common import general_filter
import logging
import sys

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['LIKES_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['LIKES_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['LIKES_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['LIKES_FILTER']['hashing_attributes'].split('|')
NODE_ID = config['LIKES_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
LIKES_MIN =  int(config['LIKES_FILTER']['min_likes'])

CURRENT_STAGE_NAME = config['LIKES_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['LIKES_FILTER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['LIKES_FILTER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['LIKES_FILTER']['next_stage_name'].split(',')

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

class LikesFilter(general_filter.GeneralFilter):
    def __init__(self):
        print(f"Estoy suscrito al topico {CURRENT_STAGE_NAME}-{NODE_ID}")
        middleware_instance = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        # middleware_instance = poisoned_middleware.PoisonedExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
        #                                             CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        query_state_instance = query_state.QueryState('/root/storage/', read_value, write_value)
        super().__init__(NODE_ID, PREVIOUS_STAGE_AMOUNT, middleware_instance, query_state_instance)

    def process_data_message(self, input_message):
        client_id = input_message['client_id']
        try:
            self.query_state.write(client_id, input_message['origin'], input_message['msg_id'])
            if int(input_message['likes']) >= LIKES_MIN:
                message_data = {k: input_message[k] for k in OUTPUT_COLUMNS}
                message_data['msg_id'] = self.query_state.get_id(client_id)
                message_data['origin'] = NODE_ID
                self.middleware.send(message_data)
        except KeyError as e:
            logging.error('Entry lacks "likes" field.')
            logging.error(input_message)
        finally:
            self.query_state.commit(client_id, input_message['origin'], str(input_message['msg_id']))

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = LikesFilter()
    wrapper.start_received_messages_processing()
    logging.info("EL BICHOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
    # del wrapper
    wrapper.stop_health_process()
    # logging.info(f'{sys.getrefcount(wrapper)}     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')
    logging.info("SIUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUUU")

if __name__ == "__main__":
    main()