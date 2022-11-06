import time
import fcntl
import csv
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
    
def filter_duplicates(input_message):
    if input_message['type'] == 'control':
        return input_message

    with open(STORAGE + input_message['video_id'], 'a+') as id_file:
        fcntl.flock(id_file, fcntl.LOCK_EX)
        id_file.seek(0)

        reader = csv.DictReader(id_file, fieldnames=['categoryId', 'title'], quotechar='"')
        for record in reader:
            if record['title'] == input_message['title'] and record['categoryId'] == input_message['categoryId']:
                fcntl.flock(id_file, fcntl.LOCK_UN)
                return None

        id_file.write('"{}","{}"\n'.format(input_message['categoryId'], input_message['title']))
        id_file.flush()
        fcntl.flock(id_file, fcntl.LOCK_UN)

    input_message['case']='unique_pair'
    return {k: input_message[k] for k in OUTPUT_COLUMNS}

def _on_last_eof(middleware, input_message):
    utils.clear_all_files(STORAGE)
    return {'type':'control', 'case':'eof'}

def message_callback(middleware, input_message):
    
    if input_message['type'] == 'data':
        return filter_duplicates(input_message)
    else:
        if input_message['case'] == 'eof':
            return broadcast_copies.broadcast_copies(middleware, input_message, ID, COPIES, None, _on_last_eof)

        return None

middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, message_callback)
middleware.run()


class DuplicationFilter:
    def __init__(self):
        self.middleware = middleware.ChannelChannelFilter(RABBIT_HOST, INPUT_QUEUE, OUTPUT_QUEUE, self.process_received_message)
        self.clients_sent_videos = {} # key: client_id, value: sent_videos_set
        self.received_eofs = 0
        self.previous_stage_size = self.middleware.get_previous_stage_size()

        signal.signal(signal.SIGTERM, self.__handle_signal)

    def process_received_message(self, ch, method, properties, body):
        line = json.loads(body)

        if method.routing_key == general_config["general_subscription_routing_key"]:
            self.received_eofs += 1
            if self.received_eofs == self.previous_stage_size:
                self.has_to_close = True
        else:
            video_id = line[local_config["indexes"]["video_id"]]
            title = line[local_config["indexes"]["title"]]
            category = line[local_config["indexes"]["category"]]
            if not (video_id in self.sent_videos):
                self.sent_videos.add(video_id)
                self.middleware.send({ "type": cluster_type, "tuple": (video_id, title, category) })

        if self.has_to_close:
            self.middleware.send_general(None)
            self.middleware.close()
            print("Closed MOM")

    def start_received_messages_processing(self):
        self.middleware.run()

    def __handle_signal(self, *args): # To prevent double closing 
        self.has_to_close = True

def main():
    # logging.basicConfig(
    #     format='%(asctime)s %(levelname)-8s %(message)s',
    #     level="DEBUG",
    #     datefmt='%Y-%m-%d %H:%M:%S',
    # )
    wrapper = DuplicationFilter()
    wrapper.start_received_messages_processing()

if __name__ == "__main__":
    main()


# class DuplicationFilter:
#     def __init__(self):
#         self.middleware = MOM(cluster_type, self.process_received_message)
#         self.sent_videos = set()
#         self.received_eofs = 0
#         self.has_to_close = False
#         self.is_processing_message = False
#         self.previous_stage_size = self.middleware.get_previous_stage_size()

#         signal.signal(signal.SIGTERM, self.__handle_signal)

#     def process_received_message(self, ch, method, properties, body):
#         line = json.loads(body)

#         if method.routing_key == general_config["general_subscription_routing_key"]:
#             self.received_eofs += 1
#             if self.received_eofs == self.previous_stage_size:
#                 self.has_to_close = True
#         else:
#             video_id = line[local_config["indexes"]["video_id"]]
#             title = line[local_config["indexes"]["title"]]
#             category = line[local_config["indexes"]["category"]]
#             if not (video_id in self.sent_videos):
#                 self.sent_videos.add(video_id)
#                 self.middleware.send({ "type": cluster_type, "tuple": (video_id, title, category) })

#         if self.has_to_close:
#             self.middleware.send_general(None)
#             self.middleware.close()
#             print("Closed MOM")

#     def start_received_messages_processing(self):
#         self.middleware.start_received_messages_processing()

#     def __handle_signal(self, *args): # To prevent double closing 
#         self.has_to_close = True

# def main():
#     # logging.basicConfig(
#     #     format='%(asctime)s %(levelname)-8s %(message)s',
#     #     level="DEBUG",
#     #     datefmt='%Y-%m-%d %H:%M:%S',
#     # )
#     wrapper = DuplicationFilter()
#     wrapper.start_received_messages_processing()

# if __name__ == "__main__":
#     main()