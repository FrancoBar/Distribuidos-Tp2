from common import middleware
from common import utils
import logging
import time
#Conexiones
# input -> extcp -> tcpex -> output_1 y output_2

utils.initialize_log("INFO")
logging.info("Input up")

def filter_func(input_message):
    logging.info(input_message)
    return input_message

def route_key_gen(message):
    return "default"# message["id"]

middleware = middleware.ExchangeExchangeFilter("rabbitmq", "input_ex","default","control","exex_ex", route_key_gen, filter_func)
time.sleep(5)
logging.info("Sending")
middleware.send({ "type":"data","id":"0" })
middleware.send({ "type":"data","id":"1" })
middleware.send({ "type":"control","case":"eof"})
middleware.send({ "type":"data","id":"0" })
middleware.send({ "type":"data","id":"1" })
