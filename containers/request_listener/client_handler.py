import logging
from common import middleware
from common import utils
from asyncio import IncompleteReadError
from common import routing
from common import query_state
import os
import socket
import time

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['REQUEST_LISTENER']['input_exchange']
OUTPUT_EXCHANGE = config['REQUEST_LISTENER']['output_exchange']
STORAGE = config['REQUEST_LISTENER']['storage']

OUTPUT_COLUMNS = config['REQUEST_LISTENER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['REQUEST_LISTENER']['hashing_attributes'].split('|')
NODE_ID = config['REQUEST_LISTENER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']

PREVIOUS_STAGES_AMOUNTS = config['REQUEST_LISTENER']['previous_stage_amount'].split(',')
NEXT_STAGE_AMOUNTS = config['REQUEST_LISTENER']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['REQUEST_LISTENER']['next_stage_name'].split(',')

# aux_client_id = 'generic_client_id'

# previous_stages_nodes = 0
previous_stages_nodes = 1

for amount in PREVIOUS_STAGES_AMOUNTS:
    previous_stages_nodes += int(amount)

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)


class ClientHandler:
    def __init__(self):
        # self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.received_eofs = 0 # key: client_id, value: number of eofs received
        self.entry_input = None
        self.entry_ouput = None
        self.client_id = None
        self.process_id = None
        self.last_received_msg = {}

    # def connection_handler(self, accept_socket, client_id):
    def handle_connection(self, process_id, accept_socket, client_id):

        try:
            self.client_id = client_id
            self.process_id = process_id
            self.msg_counter = 0
            self.entry_input = middleware.TCPExchangeFilter(RABBIT_HOST, accept_socket, OUTPUT_EXCHANGE, routing_function, self.entry_recv_callback)
            file_name = STORAGE + client_id
            if accept_socket != None:
                # Creates a file that represents the client session and allows to
                # erase temporary states on a failure.
                file_name += query_state.FILE_TYPE
                open(file_name, 'x')

                self.entry_ouput = middleware.ExchangeTCPFilter(RABBIT_HOST, INPUT_EXCHANGE, client_id, CONTROL_ROUTE_KEY, accept_socket, self.answers_callback)
                
                logging.info('Receiving entries')
                self.entry_input.run()

                # print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC")
                # time.sleep(10)

                logging.info('Answering entries')
                self.entry_ouput.run()
                
            else:
                self.entry_input.send({'type':'priority', 'case':'disconnect', 'client_id':client_id})
                print("BORRAR ENTRE A ESTE CASO AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                print(client_id)
        except [IncompleteReadError, socket.error] as e:
            logging.error('Client abruptly disconnected')
            self.entry_input.send({'type':'priority', 'case':'disconnect', 'client_id':client_id})
            logging.exception(e)
        except Exception as e:
            raise e
        finally:
            try:
                print(f"VOY A BORRAR EL ARCHIVO {file_name} EPICARDIUM")
                self.entry_input.delete_input_queue()
                os.remove(file_name)
                print(f"BORRE EL ARCHIVO {file_name} EPICARDIUM")
            except FileNotFoundError:
                pass

    def entry_recv_callback(self, input_message):
        if input_message['type'] == 'control' and input_message['case'] == 'eof':
            self.entry_input.stop()
        input_message['client_id'] = self.client_id
        input_message['origin'] = self.process_id
        input_message['msg_id'] = self.msg_counter
        self.entry_input.send(input_message)
        self.msg_counter += 1

    def answers_callback(self, input_message):
        if (input_message['type'] == 'priority'):
            logging.info(f"Received disconnected")
            # self.entry_ouput.stop()
            return

        pipeline_origin = input_message['origin']
        producer = input_message['producer']
        origin = f'{pipeline_origin},{producer}'
        msg_id = input_message['msg_id']
        
        if (origin in self.last_received_msg) and (msg_id == self.last_received_msg[origin]):
            print(f"{input_message}")
            return
        self.last_received_msg[origin] = msg_id

        if input_message['type'] == 'control':
            if input_message['case'] == 'eof':
                if ('producer' in input_message) and (input_message['producer'] == 'max_date'):
                    input_message['type'] = 'data'
                    self.entry_ouput.send(input_message)
                self.received_eofs += 1
                if self.received_eofs == previous_stages_nodes:
                    self.entry_ouput.send({ 'type': 'control', 'case': 'eof' })
                    self.entry_ouput.stop()
        else:
            self.entry_ouput.send(input_message)
            # print("BORRAR ENVIE UN MENSAJE")
