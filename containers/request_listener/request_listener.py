#!/usr/bin/env python3
import sys
import logging
from asyncio import IncompleteReadError
from common import middleware
from common import utils
from common import server

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE = config['REQUEST_LISTENER']['input_queue']
OUTPUT_QUEUE = config['REQUEST_LISTENER']['output_queue']
OUTPUT_COLUMNS = config['REQUEST_LISTENER']['output_columns'].split(',')
PORT = int(config['REQUEST_LISTENER']['port'])
FLOWS_AMOUNT = int(config['REQUEST_LISTENER']['flows_amount'])

eof_amount = 0

def entry_recv_callback(middleware, input_message):
  if input_message['type'] == 'control' and input_message['case'] == 'eof':
      middleware.stop()
  return input_message

def answers_callback(middleware, input_message):
  global eof_amount
  if input_message['type'] == 'control':
    if input_message['case'] == 'eof':
      eof_amount += 1
      if eof_amount != FLOWS_AMOUNT:
        return None
      else:
        eof_amount = 0
    else:
      return None
      
  return input_message

def connection_handler(accept_socket):
  try:
    entry_input = middleware.TCPChannelFilter(RABBIT_HOST, accept_socket, OUTPUT_QUEUE, entry_recv_callback)
    entry_ouput = middleware.ChannelTCPFilter(RABBIT_HOST, INPUT_QUEUE, accept_socket, answers_callback)
    
    logging.info('Receiving entries')
    entry_input.run()

    logging.info('Answering entries')
    entry_ouput.run()

  except IncompleteReadError as e:
    logging.error('Client abruptly disconnected')
    logging.exception(e)
  except Exception as e:
    raise e

server = server.Server(PORT, 1, connection_handler)
server.run()
