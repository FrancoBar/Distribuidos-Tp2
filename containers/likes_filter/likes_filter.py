import time
from common import middleware
from common import utils
import logging

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['LIKES_FILTER']['input_queue']
OUTPUT_QUEUE = config['LIKES_FILTER']['output_queue']
OUTPUT_COLUMNS = config['LIKES_FILTER']['output_columns'].split(',')
LIKES_MIN =  int(config['LIKES_FILTER']['min_likes'])

def filter_likes(middleware, input_message):
    if input_message['type'] == 'control':
        return input_message

    try:
        if int(input_message['likes']) >= LIKES_MIN:
            return {k: input_message[k] for k in OUTPUT_COLUMNS}
    except KeyError as e:
        logging.error('Entry lacks "likes" field.')
        logging.error(input_message)
    
    return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, filter_likes)
middleware.run()



class LikesFilter:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_sent_videos = {} # key: client_id, value: sent_videos_tuples_set
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        self.previous_stage_size = self.middleware.get_previous_stage_size()


    def filter_likes(self, input_message):
        try:
            if int(input_message['likes']) >= LIKES_MIN:
                return {k: input_message[k] for k in OUTPUT_COLUMNS}
        except KeyError as e:
            logging.error('Entry lacks "likes" field.')
            logging.error(input_message)
        
        return None

    def process_received_message(self, input_message):
        if input_message['type'] == 'control':
            return input_message

        # If not eof
        return self.filter_likes(self, input_message)

        # If client eof
            # volar todo y enviar

    def start_received_messages_processing(self):
        self.middleware.run()

    def __handle_signal(self, *args): # To prevent double closing 
        self.has_to_close = True

def main():
    wrapper = LikesFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()