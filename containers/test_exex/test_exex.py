from common import middleware
from common import utils
import logging

utils.initialize_log("INFO")
logging.info("Exex up")

def filter_func(input_message):
    return input_message

def route_key_gen(message):
    return "default"# message["id"]

middleware = middleware.ExchangeExchangeFilter("rabbitmq", "exex_ex","default","control","extcp_ex", route_key_gen, filter_func)
middleware.run()