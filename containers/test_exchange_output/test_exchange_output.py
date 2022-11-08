from common import middleware
from common import utils
import logging
import os

id = os.environ['ID']
utils.initialize_log("INFO")
logging.info("Output up")

def print_message(middleware, input_message):
    logging.info(input_message)
    return input_message

middleware = middleware.ExchangeExchangeFilter("rabbitmq", "output_ex", id, "control", "o", lambda x: "1", print_message)
middleware.run()