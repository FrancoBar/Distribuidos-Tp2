import sys
import time
import csv
import json
import os
import multiprocessing
import socket
import base64
import signal
from asyncio import IncompleteReadError
from common import transmition

USED_COLUMNS = ('type', 'categoryId', 'likes', 'title', 'tags', 'trending_date', 'video_id', 'view_count', 'country', 'thumbnail_link')
ID_DIRECTORY='client_' + os.environ['NODE_ID'] + '/'
STORAGE='./output/'

if not os.path.exists(STORAGE + 'thumbnails/'):
    os.makedirs(STORAGE + 'thumbnails/')

def file_process(file_name, client_socket, lock):
    with open('./datasets/' + file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        while(True):
            try:
                row = next(reader, None)
                if not row:
                    print('Thread end')
                    break
                row['type'] = 'data'
                row['country'] = file_name[:2]
                subset = {k: row[k] for k in USED_COLUMNS}
                message = json.dumps(subset, indent = 4)

                lock.acquire()
                transmition.send_str(client_socket, message)
                lock.release()
            except IncompleteReadError as e:
                lock.release()
                print(e)
                sys.exit(1)
            except Exception as e:
                print(e)
                sys.exit(1)

def recv_answer(client_socket):
    while True:
        try:
            message_str = transmition.recv_str(client_socket)
            message = json.loads(message_str)

            if message['type'] == 'control' and message['case'] == 'eof':
                break
            if message['producer'] == 'img':
                img_data = base64.b64decode(message['img_data'])
                with open(STORAGE + ID_DIRECTORY + 'thumbnails/' + message['video_id'] + '.jpg', 'wb') as thumbnail_file:
                    thumbnail_file.write(img_data)
            elif message['producer'] == 'unique_pair':
                with open(STORAGE + ID_DIRECTORY + 'unique_pairs.txt', 'a') as unique_pairs_file:
                    unique_pairs_file.write('"{}","{}","{}"\n'.format(message['video_id'], message['title'], message['categoryId']))
        
            elif message['producer'] == 'max_date':
                with open(STORAGE + ID_DIRECTORY + 'max_date.txt', 'a') as unique_pairs_file:
                    unique_pairs_file.write('"{}","{}"\n'.format(message['date'], message['view_count']))

        except Exception as e:
            print(e)
            sys.exit(1)

HOST = os.environ['SERVER_HOST']
PORT = int(os.environ['SERVER_PORT'])

def signal_handler(signum, frame):
    print('SIGTERM received')
    try:
        sys.exit(0)
        client_socket.close()
    except SystemExit as e:
        os._exit(0)

signal.signal(signal.SIGTERM, signal_handler)

print('Client up')

csv_files_list = list(filter(lambda file_name : file_name[-4:] == '.csv', os.listdir('./datasets')))

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST, PORT))

data = json.dumps({'type': 'control', 'case':'config', 'amount_countries': len(csv_files_list)})
transmition.send_str(client_socket, data)

print('Start sending...')
lock = multiprocessing.Lock()
process_list = []
for file_name in csv_files_list:
    p = multiprocessing.Process(target=file_process, args=[file_name, client_socket, lock])
    p.start()
    process_list.append(p)

for p in process_list:
    p.join()

print('Start receiving...')
message = json.dumps({'type':'control', 'case':'eof'}, indent = 4)
transmition.send_str(client_socket, message)
recv_answer(client_socket)
client_socket.close()
print('End')
