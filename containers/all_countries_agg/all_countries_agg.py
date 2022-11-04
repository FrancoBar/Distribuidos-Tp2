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

def all_countries_agg(input_message):
    global amount_countries
    temp=time.strptime(input_message['trending_date'], '%Y-%m-%dT%H:%M:%SZ')
    input_message['trending_date']=time.strftime('%Y-%m-%d',temp)

    with open(STORAGE + input_message['video_id'], 'a+') as id_file:
        fcntl.flock(id_file, fcntl.LOCK_EX)
        id_file.seek(0)
        output_message = None
        country_amounts = {input_message['trending_date']:1}
        reader = csv.DictReader(id_file, fieldnames=['trending_date','country'])
        for record in reader:
            if record['trending_date'] not in country_amounts:
                country_amounts[record['trending_date']] = 0
            country_amounts[record['trending_date']] += 1
            #Exit if already considered
            if record['trending_date'] == input_message['trending_date'] and record['country'] == input_message['country']:
                    fcntl.flock(id_file, fcntl.LOCK_UN)
                    return None
        trending_days = sum(list(map(lambda x: 1 if x == amount_countries  else 0, country_amounts.values())))
        if trending_days <= MIN_DAYS:
            id_file.write('{},{}\n'.format(input_message['trending_date'], input_message['country']))
            id_file.flush()
            if trending_days == MIN_DAYS and country_amounts[input_message['trending_date']] == amount_countries:
                #First message
                output_message = {k: input_message[k] for k in OUTPUT_COLUMNS}
        fcntl.flock(id_file, fcntl.LOCK_UN)
    return output_message

def _on_recv_eof(middleware, input_message):
    global amount_countries
    amount_countries = INVALID_AMOUNT
    return None

def _on_last_eof(middleware, input_message):
    utils.clear_all_files(STORAGE)
    return {'type':'control', 'case':'eof'}

def _on_recv_config(middleware, input_message):
    global amount_countries
    amount_countries = int(input_message['amount_countries'])
    return None

def message_callback(middleware, input_message):
    if input_message['type'] == 'data':
        if amount_countries == INVALID_AMOUNT:
            #Put back data message until config message
            middleware.put_back(input_message)
            return None
        else:
            return all_countries_agg(input_message)
    else:
        if input_message['case'] == 'eof':
            return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, _on_recv_eof, _on_last_eof)
        else:
            return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, _on_recv_config, None)
            
        return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, message_callback)
middleware.run()
