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
NODE_ID = config['ALL_COUNTRIES_AGG']['node_id']
CONTROL_ROUTE_KEY = config['GENERAL']['control_route_key']
PORT = int(config['ALL_COUNTRIES_AGG']['port'])
FLOWS_AMOUNT = int(config['ALL_COUNTRIES_AGG']['flows_amount'])
MIN_DAYS = int(config['ALL_COUNTRIES_AGG']['min_days'])

CURRENT_STAGE_NAME = config['ALL_COUNTRIES_AGG']['current_stage_name']
PREVIOUS_STAGE_AMOUNT = config['ALL_COUNTRIES_AGG']['previous_stage_amount']
HASHING_ATTRIBUTES = config['ALL_COUNTRIES_AGG']['hashing_attributes'].split('|')
NEXT_STAGE_AMOUNTS = config['ALL_COUNTRIES_AGG']['next_stage_amount'].split(',')
NEXT_STAGE_NAMES = config['ALL_COUNTRIES_AGG']['next_stage_name'].split(',')


# def generate_routing_function(next_stage_names, hashing_attributes, next_stage_amounts):
#     stages_rounting_data = []
#     for i in range(len(NEXT_STAGE_NAMES)):
#         stages_rounting_data.append({ 
#             "next_stage_name": next_stage_names[i], 
#             "hashing_attributes": hashing_attributes[i].split(','), 
#             "next_stage_amount": int(next_stage_amounts[i])
#         })
#     return lambda message: routing.router_iter(message, CONTROL_ROUTE_KEY, stages_rounting_data)

# stages_rounting_data = []
# for i in range(len(NEXT_STAGE_NAMES)):
#     stages_rounting_data.append({ 
#         "next_stage_name": NEXT_STAGE_NAMES[i], 
#         "hashing_attributes": HASHING_ATTRIBUTES[i].split(','), 
#         "next_stage_amount": int(NEXT_STAGE_AMOUNTS[i])
#     })

def router(message):
    return routing.router_iter(message, CONTROL_ROUTE_KEY, stages_rounting_data)


class CountriesAmountFilter:
    def __init__(self):
        self.middleware = middleware.ExchangeExchangeFilter(RABBIT_HOST, INPUT_EXCHANGE, OUTPUT_EXCHANGE, f'{CURRENT_STAGE_NAME}-{NODE_ID}', 
                                                    CONTROL_ROUTE_KEY, OUTPUT_EXCHANGE, router, self.process_received_message)
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.clients_countries_per_day = {} # key: client_id, value: {key: video_id, value: { key: day, value: countries set}}
        self.clients_countries_amount = {} # key: client_id, value: countries_amount

    def process_control_message(self, input_message):
        client_id = input_message['client_id']
        if input_message['case'] == 'eof':
            self.clients_received_eofs[client_id] += 1
            if self.clients_received_eofs[client_id] == PREVIOUS_STAGE_AMOUNT:
                del self.clients_countries_amount[client_id]
                del self.clients_countries_per_day[client_id]
                del self.clients_received_eofs[client_id]
                return input_message
            else:
                return None
        else:
            self.clients_countries_amount[client_id] = int(input_message['amount_countries'])
            return input_message

    def all_countries_agg(self, input_message):
        client_id = input_message['client_id']
        client_countries_amount = self.clients_countries_amount[client_id]
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
        client_id = input_message['client_id']
        message_to_send = None

        # Initialization
        if not (client_id in self.clients_received_eofs):
            self.clients_received_eofs[client_id] = 0
            self.clients_countries_per_day[client_id] = {}

        # Message processing
        if input_message['type'] == 'data':
            message_to_send = self.all_countries_agg(input_message)
        else:
            message_to_send = self.process_control_message(input_message)

        # Result communication
        if message_to_send != None:
            self.middleware.send(message_to_send)


    def start_received_messages_processing(self):
        self.middleware.run()

def main():
    wrapper = CountriesAmountFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()