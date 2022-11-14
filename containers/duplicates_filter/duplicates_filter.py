import os
from common import broadcast_copies
from common import middleware
from common import utils
from common import routing
import logging

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['DUPLICATES_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['DUPLICATES_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['DUPLICATES_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['DUPLICATES_FILTER']['hashing_attributes'].split(',')
NODE_ID = config['DUPLICATES_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['DUPLICATES_FILTER']['port'])
FLOWS_AMOUNT = int(config['DUPLICATES_FILTER']['flows_amount'])

PREVIOUS_STAGE_AMOUNT = config['DUPLICATES_FILTER']['previous_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT = config['DUPLICATES_FILTER']['next_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_NAME = config['DUPLICATES_FILTER']['next_stage_name'] # Hacer un for de las etapas anteriores
    
class DuplicationFilter:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, NODE_ID, 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.router, self.process_received_message)
        self.clients_sent_videos = {} # key: client_id, value: sent_videos_tuples_set
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()

    def filter_duplicates(self, input_message):
            # logging.warning(f"BORRAR ENTRE A FILTER DUPLICATES")
            video_id = input_message['video_id']
            title = input_message['title']
            category = input_message['categoryId']
            client_id = input_message['client_id']
            if not (client_id in self.clients_sent_videos):
                self.clients_sent_videos[client_id] = set()
            client_set = self.clients_sent_videos[client_id]
            # video_tuple = (video_id, title, category)
            video_tuple = f"{video_id},{title},{category}"
            # if not (video_tuple in self.clients_sent_videos):
            if not (video_tuple in client_set):
                client_set.add(video_tuple) #BORRAR COMENTARIO: en caso de que falle usar un string con los datos concatenados
                # self.clients_sent_videos[client_id].add(video_tuple) #BORRAR COMENTARIO: en caso de que falle usar un string con los datos concatenados
                input_message['case']='unique_pair'
                # logging.warning(f"BORRAR {client_set}")
                return {k: input_message[k] for k in OUTPUT_COLUMNS}
            else:
                return None

    def _on_last_eof(self, input_message):
        # utils.clear_all_files(STORAGE)
        print("SOY _on_last_eof")
        return {'type':'control', 'case':'eof'}

    #BORRAR: ver si creamos una clase abstracta de la que heredan todas las clases de 
    def process_control_message(self, input_message):
        client_id = input_message['client_id']

        if input_message['case'] == 'eof':
            self.clients_received_eofs[client_id] += 1
            if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:
                del self.clients_sent_videos[client_id]
                del self.clients_received_eofs[client_id]
                return input_message
        return None


    def process_received_message(self, input_message):
        client_id = input_message['client_id']
        message_to_send = None

        if not (client_id in self.clients_received_eofs):
            self.clients_received_eofs[client_id] = 0
            self.clients_sent_videos[client_id] = set()

        if input_message['type'] == 'data':
            message_to_send = self.filter_duplicates(input_message)
        else:
            message_to_send = self.process_control_message(input_message)

        if message_to_send != None:
            self.middleware.send(message_to_send)

    def start_received_messages_processing(self):
        self.middleware.run()


def main():
    wrapper = DuplicationFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()
