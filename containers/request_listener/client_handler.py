import logging
from common import middleware
from common import utils
from asyncio import IncompleteReadError
from common import routing

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['REQUEST_LISTENER']['input_exchange']
OUTPUT_EXCHANGE = config['REQUEST_LISTENER']['output_exchange']


OUTPUT_COLUMNS = config['REQUEST_LISTENER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['REQUEST_LISTENER']['hashing_attributes'].split('|')
NODE_ID = config['REQUEST_LISTENER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']

CURRENT_STAGE_NAME = config['REQUEST_LISTENER']['current_stage_name']
PREVIOUS_STAGES_AMOUNTS = config['REQUEST_LISTENER']['previous_stage_amount'].split(',')
NEXT_STAGE_AMOUNTS = config['REQUEST_LISTENER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['REQUEST_LISTENER']['next_stage_name'].split(',')

# aux_client_id = 'generic_client_id'

previous_stages_nodes = 0

for amount in PREVIOUS_STAGES_AMOUNTS:
    previous_stages_nodes += int(amount)
    # TODO: IMPLEMENT THE WAIT FOR 1 EOF FROM THE MAX AGGREGATOR STAGE 

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)


class ClientHandler:
    def __init__(self):
        # self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.received_eofs = 0 # key: client_id, value: number of eofs received
        self.entry_input = None
        self.entry_ouput = None
        self.client_id = None

    # def connection_handler(self, accept_socket, client_id):
    def handle_connection(self, accept_socket, client_id):
        try:
            self.client_id = client_id
            self.entry_input = middleware.TCPExchangeFilter(RABBIT_HOST, accept_socket, OUTPUT_EXCHANGE, routing_function, self.entry_recv_callback)
            # self.entry_ouput = middleware.ExchangeTCPFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', CONTROL_ROUTE_KEY, accept_socket, self.answers_callback)
            self.entry_ouput = middleware.ExchangeTCPFilter(RABBIT_HOST, INPUT_EXCHANGE, client_id, CONTROL_ROUTE_KEY, accept_socket, self.answers_callback)
            
            logging.info('Receiving entries')
            self.entry_input.run()

            logging.info('Answering entries')
            self.entry_ouput.run()

        except IncompleteReadError as e:
            logging.error('Client abruptly disconnected')
            logging.exception(e)
        except Exception as e:
            raise e

    def entry_recv_callback(self, input_message):
        if input_message['type'] == 'control' and input_message['case'] == 'eof':
            self.entry_input.stop()
        input_message['client_id'] = self.client_id

        # # BORRAR
        # if input_message['type'] == 'control':
        #     print(f"BORRAR mensaje de control: {input_message}")

        self.entry_input.send(input_message)

    def answers_callback(self, input_message):
        # print(f"BORRAR me llego el mensaje {input_message}")
        # client_id = input_message['client_id']
        if input_message['type'] == 'control':
            if input_message['case'] == 'eof':
                if ('producer' in input_message) and (input_message['producer'] == 'max_date'):
                    input_message['type'] = 'data'
                    self.entry_ouput.send(input_message)
                # if not (client_id in self.received_eofs):
                #     self.received_eofs[client_id] = 0
                # self.received_eofs[client_id] += 1
                self.received_eofs += 1
                print(f"BORRAR Me llego un eof {input_message}")
                # if self.received_eofs[client_id] == previous_stages_nodes:
                if self.received_eofs == previous_stages_nodes:
                    print("BORRAR Termino todo")
                    # self.entry_ouput.send(input_message)
                    self.entry_ouput.send({ 'type': 'control', 'case': 'eof' })
                    # del self.received_eofs[client_id]
                    self.entry_ouput.stop()
        else:
            self.entry_ouput.send(input_message)
