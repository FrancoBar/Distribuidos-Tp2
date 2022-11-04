import os
from common import middleware
from common import utils

SERVICE_NAME = os.environ['SERVICE_NAME']

config = utils.initialize_config()

LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

RABBIT_HOST = config['RABBIT']['address']
INPUT_QUEUE = config[SERVICE_NAME]['input_queue']
OUTPUT_QUEUE = config[SERVICE_NAME]['output_queue']
OUTPUT_QUEUE2 = config[SERVICE_NAME]['output_queue_2']

middleware = middleware.Adaptor(RABBIT_HOST, INPUT_QUEUE, [OUTPUT_QUEUE, OUTPUT_QUEUE2])
middleware.run()
