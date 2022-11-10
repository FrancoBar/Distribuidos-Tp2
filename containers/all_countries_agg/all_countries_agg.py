import time
import os
import csv
import fcntl
from common import broadcast_copies
from common import middleware
from common import utils
from common import routing

INVALID_AMOUNT = -1

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)



RABBIT_HOST = config['RABBIT']['address']
INPUT_EXCHANGE = config['ALL_COUNTRIES_AGG']['input_exchange']
OUTPUT_EXCHANGE = config['ALL_COUNTRIES_AGG']['output_exchange']
OUTPUT_COLUMNS = config['ALL_COUNTRIES_AGG']['output_columns'].split(',')
HASHING_ATTRIBUTES = config['ALL_COUNTRIES_AGG']['hashing_attributes'].split(',')
NODE_ID = config['ALL_COUNTRIES_AGG']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['ALL_COUNTRIES_AGG']['port'])
FLOWS_AMOUNT = int(config['ALL_COUNTRIES_AGG']['flows_amount'])

PREVIOUS_STAGE_AMOUNT = config['ALL_COUNTRIES_AGG']['previous_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_AMOUNT = config['ALL_COUNTRIES_AGG']['next_stage_amount'] # Hacer un for de las etapas anteriores
NEXT_STAGE_NAME = config['ALL_COUNTRIES_AGG']['next_stage_name'] # Hacer un for de las etapas anteriores

aux_client_id = 'generic_client_id'

class CountriesAmountFilter:
    def __init__(self):
        # self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, NODE_ID, 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, routing.router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()
        self.clients_countries_per_day = {} # key: client_id, value: {key: video_id, value: { key: day, value: countries set}}
        self.clients_countries_amount = {} # key: client_id, value: countries_amount

    def _on_recv_eof(self, input_message):
        client_id = aux_client_id
        if client_id in self.clients_countries_amount:
            del self.clients_countries_amount[client_id]
        if client_id in self.clients_countries_per_day:
            del self.clients_countries_per_day[client_id]
    
        # del self.clients_max_day[client_id]
        return None

    def _on_last_eof(self, input_message):
        utils.clear_all_files(STORAGE)
        return {'type':'control', 'case':'eof'}

    def _on_recv_config(self, input_message):
        # print("SETEE EL CONFIG AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAV")
        client_id = aux_client_id
        self.clients_countries_amount[client_id] = int(input_message['amount_countries'])
        return None

    def all_countries_agg(self, input_message):
        client_id = aux_client_id
        client_countries_amount = self.clients_countries_amount[client_id]
        if not (client_id in self.clients_countries_per_day):
            self.clients_countries_per_day[client_id] = {}
        client_trending_days_dict = self.clients_countries_per_day[client_id]

        temp=time.strptime(input_message['trending_date'], '%Y-%m-%dT%H:%M:%SZ')
        input_message['trending_date']=time.strftime('%Y-%m-%d',temp)
        used_date = input_message['trending_date']
        country = input_message['country']
        video_id = input_message['video_id']
        output_message = None

        if not (video_id in client_trending_days_dict):
            client_trending_days_dict[video_id] = {}
        video_dates = client_trending_days_dict[video_id]

        if not (used_date in video_dates):
            video_dates[used_date] = set()

        if country in video_dates[used_date]:
            return None
                
        video_dates[used_date].add(country)
        date_countries_amount = len(video_dates[used_date])

        all_countries_trending_days_amount = 0
        for date in video_dates:
            if len(video_dates[date]) == client_countries_amount:
                all_countries_trending_days_amount += 1

        if all_countries_trending_days_amount > MIN_DAYS:
            return None
        elif (all_countries_trending_days_amount == MIN_DAYS) and (date_countries_amount == client_countries_amount):
           output_message = {k: input_message[k] for k in OUTPUT_COLUMNS}

        return output_message

    def process_received_message(self, input_message):
        client_id = aux_client_id
        if input_message['type'] == 'data':
            if not (client_id in self.clients_countries_amount):
            # if amount_countries == INVALID_AMOUNT:
                #Put back data message until config message
                self.middleware.put_back(input_message)
                return None
            else:
                return self.all_countries_agg(input_message)
        else:
            if input_message['case'] == 'eof':
                return broadcast_copies.broadcast_copies(self.middleware, input_message, ID, COPIES, self._on_recv_eof, self._on_last_eof)
            else:
                return broadcast_copies.broadcast_copies(self.middleware, input_message, ID, COPIES, self._on_recv_config, None)
                
            return None

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = CountriesAmountFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()