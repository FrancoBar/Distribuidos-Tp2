import json
import time
from common import middleware
from common import utils
from common import routing

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

routing_function = routing.generate_routing_function(CONTROL_ROUTE_KEY, NEXT_STAGE_NAMES, HASHING_ATTRIBUTES, NEXT_STAGE_AMOUNTS)

class MaxDayAggregator:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing_function, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.max_date = {} # key: client_id, value: [None, 0]

    # def filter_max_agg(self, input_message, client_id):
    #     amount_new = int(input_message['view_count'])
    #     if self.max_date[client_id][1] <= amount_new:
    #         self.max_date[client_id][0] = input_message['date']
    #         self.max_date[client_id][1] = amount_new
    #     return None

    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        if input_message['case'] == 'eof':
            self.clients_received_eofs[client_id] += 1
            amount_new = int(input_message['view_count'])
            if self.max_date[client_id][1] <= amount_new:
                self.max_date[client_id][0] = input_message['date']
                self.max_date[client_id][1] = amount_new

            if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:
                # output_message = {'type':'data', 'case':'max_date', 'client_id': client_id, 'date': self.max_date[client_id][0], 'view_count':self.max_date[client_id][1]}
                output_message = {'type':'control', 'case': 'eof', 'producer':'max_date', 'client_id': client_id, 'date': self.max_date[client_id][0], 'view_count':self.max_date[client_id][1]}
                del self.clients_received_eofs[client_id]
                del self.max_date[client_id]
                # self.middleware.send(output_message)
                # return {'type':'control', 'case':'eof', 'client_id': client_id}
                return output_message
        return None


# me llega con id i de la etapa anterior


# envio j enviando la fecha maxima
# envio j+1 enviando eof
# commit

# persisto id

    def process_received_message(self, input_message):
        client_id = input_message['client_id']
        message_to_send = None

        if not (client_id in self.max_date):
            self.max_date[client_id] = [None, 0]
            self.clients_received_eofs[client_id] = 0

        if input_message['type'] == 'data':
            # self.filter_max_agg(input_message, client_id)
            print("BORRAR no deberia llegar a este caso")
        else:
            print(f"BORRAR me llego el mensaje {input_message}")
            message_to_send = self.process_control_message(input_message)
            print(f"BORRAR envie el mensaje {input_message}")

        if message_to_send != None:
            self.middleware.send(message_to_send)

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = MaxDayAggregator()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()