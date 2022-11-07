from common import middleware
import logging
from common import utils

utils.initialize_log("INFO")

def filter_likes(middleware, input_message):
        
    return None

def test_function(message):
    return message["id"]

middleware = middleware.ExchangeExchangeFilter("rabbitmq", "asdasdsa", "1", "control", "i", test_function, filter_likes)

middleware.send({ "id":"0" })
middleware.send({ "id":"1" })