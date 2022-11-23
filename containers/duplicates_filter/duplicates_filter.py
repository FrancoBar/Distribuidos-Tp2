import os
# from common import broadcast_copies
from common import middleware
from common import utils
from common import routing
from common import query_state
import logging

ID=os.environ['HOSTNAME']
# COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['DUPLICATES_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['DUPLICATES_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['DUPLICATES_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['DUPLICATES_FILTER']['hashing_attributes'].split('|')
NODE_ID = config['DUPLICATES_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']

CURRENT_STAGE_NAME = config['DUPLICATES_FILTER']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = int(config['DUPLICATES_FILTER']['previous_stage_amount'])
NEXT_STAGE_AMOUNTS = config['DUPLICATES_FILTER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['DUPLICATES_FILTER']['next_stage_name'].split(',')

# routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)

# last_stage_router

def read_value(query, key, value):
    if key == 'data':
        if not (key in query):
            query[key] = set()
        query[key].add(value)
    else:
        if not (key in query):
            query[key] = 0
        query[key] += 1

def write_value(query, key, value):
    return str(value)


class DuplicationFilter:
    def __init__(self):
        # self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
        #                                             CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.last_stage_router, self.process_received_message)                                                    
        # self.clients_sent_videos = {} # key: client_id, value: sent_videos_tuples_set
        # self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.query_state = query_state.QueryState('./storage', read_value, write_value)

    def filter_duplicates(self, input_message):
            video_id = input_message['video_id']
            title = input_message['title']
            category = input_message['categoryId']
            client_id = input_message['client_id']
            client_values = self.query_state.get_values(client_id)
            
            # if not (client_id in self.clients_sent_videos):
            #     self.clients_sent_videos[client_id] = set()
            # client_set = self.clients_sent_videos[client_id]
            # video_tuple = f"{video_id},{title},{category}"
            # if not (video_tuple in client_set):
            #     client_set.add(video_tuple)
            #     input_message['producer']='unique_pair'
            #     return {k: input_message[k] for k in OUTPUT_COLUMNS}
            # else:
            #     return None
            if not ('data' in client_values):
                client_values['data'] = set()
            video_tuple = f"{video_id},{title},{category}"
            if not (video_tuple in client_values['data']):
                client_values['data'].add(video_tuple)
                input_message['producer']='unique_pair'
                self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'data', video_tuple)
                # return {k: input_message[k] for k in OUTPUT_COLUMNS}
                message_data = {k: input_message[k] for k in OUTPUT_COLUMNS}
                message_data['msg_id'] = self.query_state.get_id(client_id)
                message_data['origin'] = NODE_ID
                self.middleware.send(message_data)
            else:
                self.query_state.write(client_id, input_message['origin'], input_message['msg_id'])


    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        client_values = self.query_state.get_values(client_id)

        if input_message['case'] == 'eof':
            print(f"client values: {client_values}")
            if len(client_values) == 0:
                self.query_state.delete_query(client_id)
                return
            if not ('eof' in client_values):
                client_values['eof'] = 0
            client_values['eof'] += 1
            self.query_state.write(client_id, input_message['origin'], input_message['msg_id'], 'eof', client_values['eof'])
            # if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:

            if client_values['eof'] == PREVIOUS_STAGE_AMOUNT:
                # del self.clients_sent_videos[client_id]
                # del self.clients_received_eofs[client_id]
                input_message['msg_id'] = self.query_state.get_id(client_id)
                input_message['origin'] = NODE_ID
                self.middleware.send(input_message)
                self.query_state.delete_query(client_id)
        else:
            client_values['config'] = None


    def process_received_message(self, input_message):
        client_id = input_message['client_id']
        # message_to_send = None

        # print("BORRAR me llego un mensaje al duplicates")

        # if not (client_id in self.clients_received_eofs):
        #     self.clients_received_eofs[client_id] = 0
        #     self.clients_sent_videos[client_id] = set()

        if self.query_state.is_last_msg(client_id, input_message['origin'], str(input_message['msg_id'])):
            return

        if input_message['type'] == 'data':
            # message_to_send = self.filter_duplicates(input_message)
            self.filter_duplicates(input_message)
        else:
            # message_to_send = self.process_control_message(input_message)
            self.process_control_message(input_message)

        self.query_state.commit(client_id, input_message['origin'],str(input_message['msg_id']))

        # if message_to_send != None:
        #     self.middleware.send(message_to_send)

    def start_received_messages_processing(self):
        self.middleware.run()


def main():
    wrapper = DuplicationFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()
