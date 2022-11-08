import os
from common import broadcast_copies
from common import middleware
from common import utils

ID=os.environ['HOSTNAME']
COPIES=int(os.environ['COPIES'])

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE  = config['DUPLICATES_FILTER']['input_queue']
OUTPUT_QUEUE = config['DUPLICATES_FILTER']['output_queue']
OUTPUT_COLUMNS = config['DUPLICATES_FILTER']['output_columns'].split(',')
STORAGE = config['DUPLICATES_FILTER']['storage']
    
class DuplicationFilter:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_sent_videos = {} # key: client_id, value: sent_videos_tuples_set
        self.clients_received_eofs = {} # key: client_id, value: number of eofs received
        # self.previous_stage_size = self.middleware.get_previous_stage_size()

    def filter_duplicates(self, input_message, client_id):
            video_id = input_message['video_id']
            title = input_message['title']
            category = input_message['categoryId']
            if not (client_id in self.clients_sent_videos):
                self.clients_sent_videos[client_id] = set()
            client_set = self.clients_sent_videos[client_id]
            video_tuple = (video_id, title, category)
            if not (video_tuple in client_set):
                client_set.add(video_tuple) #BORRAR COMENTARIO: en caso de que falle usar un string con los datos concatenados
                input_message['case']='unique_pair'
                return {k: input_message[k] for k in OUTPUT_COLUMNS}
            else:
                return None

    def _on_last_eof(self, input_message):
        utils.clear_all_files(STORAGE)
        return {'type':'control', 'case':'eof'}

    #BORRAR: ver si creamos una clase abstracta de la que heredan todas las clases de 
    def process_eof(self, input_message, client_id):
        # if not (client_id in self.clients_received_eofs):
        #     self.clients_received_eofs[client_id] = 1
        # else:
        #     self.clients_received_eofs[client_id] += 1
            
        # if self.clients_received_eofs[client_id] == self.previous_stage_size:
        #     return broadcast_copies.broadcast_copies(self.middleware, input_message, ID, COPIES, None, self._on_last_eof)
        # return None #BORRAR: chequear que retornamos en este caso

        return broadcast_copies.broadcast_copies(self.middleware, input_message, ID, COPIES, None, self._on_last_eof)


    def process_received_message(self, input_message):
        client_id = 'generic_client_id'
        if input_message['type'] == 'data':
            return self.filter_duplicates(input_message, client_id)
        else:
            if input_message['case'] == 'eof':
                return self.process_eof(input_message, client_id)
            return None

    def start_received_messages_processing(self):
        self.middleware.run()


def main():
    wrapper = DuplicationFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()
