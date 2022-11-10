import json
import time
from common import middleware
from common import utils
from common import routing

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['MAX_DAY_FILTER']['input_exchange']
OUTPUT_EXCHANGE = config['MAX_AGG_FILTER']['output_exchange']
OUTPUT_COLUMNS = config['MAX_AGG_FILTER']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['MAX_AGG_FILTER']['hashing_attributes'].split(',')
NODE_ID = config['MAX_AGG_FILTER']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['MAX_AGG_FILTER']['port'])
FLOWS_AMOUNT = int(config['MAX_AGG_FILTER']['flows_amount'])

PREVIOUS_STAGE_AMOUNT = config['MAX_AGG_FILTER']['previous_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT = config['MAX_AGG_FILTER']['next_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_NAME = config['MAX_AGG_FILTER']['next_stage_name'] # Hacer un for de las etapas anteriores

class MaxDayAggregator:
    def __init__(self):
        # self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, NODE_ID, 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()
        self.max_date = {} # key: client_id, value: [None, 0]


    def filter_max_agg(self, input_message, client_id):
        amount_new = int(input_message['view_count'])
        if self.max_date[client_id][1] <= amount_new:
            self.max_date[client_id][0] = input_message['date']
            self.max_date[client_id][1] = amount_new
        return None


    def process_received_message(self, input_message):
        client_id = 'generic_client_id'
        if not (client_id in self.max_date):
            self.max_date[client_id] = [None, 0]
        if input_message['type'] == 'data':
            return self.filter_max_agg(input_message, client_id)
        else:
            if input_message['case'] != 'eof':
                return None

            output_message = {'type':'data', 'case':'max_date', 'client_id': client_id, 'date': self.max_date[client_id][0], 'view_count':self.max_date[client_id][1]}
            self.middleware.send(output_message)
            del self.max_date[client_id]

            return {'type':'control', 'case':'eof', 'client_id': client_id}


    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = MaxDayAggregator()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()