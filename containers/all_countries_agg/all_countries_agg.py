import time
import os
import csv
import fcntl
from common import broadcast_copies
from common import middleware
from common import utils

INVALID_AMOUNT = -1

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['ALL_COUNTRIES_AGG']['input_queue']
OUTPUT_QUEUE = config['ALL_COUNTRIES_AGG']['output_queue']
OUTPUT_COLUMNS = config['ALL_COUNTRIES_AGG']['output_columns'].split(',') 
MIN_DAYS = int(config['ALL_COUNTRIES_AGG']['min_days'])
STORAGE = config['ALL_COUNTRIES_AGG']['storage']
amount_countries = INVALID_AMOUNT

aux_client_id = 'generic_client_id'

class CountriesAmountFilter:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()
        self.clients_countries_per_day = {} # key: client_id, value: { key: day, value: countries set}
        self.clients_countries_amount = {} # key: client_id, value: countries_amount

    def _on_recv_eof(self, input_message):
        client_id = aux_client_id
        del self.clients_countries_amount[client_id]
        del self.clients_max_day[client_id]
        return None

    def _on_last_eof(self, input_message):
        utils.clear_all_files(STORAGE)
        return {'type':'control', 'case':'eof'}

    def _on_recv_config(self, input_message):
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
        output_message = None

        all_countries_trending_days_amount = 0
        for date in client_trending_days_dict:
            if len(client_trending_days_dict[date]) == client_countries_amount:
                all_countries_trending_days_amount += 1

        if not (used_date in client_trending_days_dict):
            client_trending_days_dict[used_date] = set()
        current_date_previous_countries_amount = len(client_trending_days_dict[used_date])
        client_trending_days_dict[used_date].add(input_message['country'])
        new_current_date_countries_amount = len(client_trending_days_dict[used_date])
        if ((new_current_date_countries_amount == client_countries_amount) and 
            (new_current_date_countries_amount != current_date_previous_countries_amount) and
            (all_countries_trending_days_amount + 1 == MIN_DAYS)):
           output_message = {k: input_message[k] for k in OUTPUT_COLUMNS}

        return output_message

    def process_received_message(self, input_message):

        if input_message['type'] == 'data':
            if amount_countries == INVALID_AMOUNT:
                #Put back data message until config message
                self.middleware.put_back(input_message)
                return None
            else:
                return self.all_countries_agg(input_message)
        else:
            if input_message['case'] == 'eof':
                return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, self._on_recv_eof, self._on_last_eof)
            else:
                return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, self._on_recv_config, None)
                
            return None

    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = CountriesAmountFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()