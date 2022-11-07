from common import middleware
import os

id = os.environ['ID']

def filter_likes(middleware, input_message):
    print(input_message)   
    return input_message

middleware = middleware.ExchangeExchangeFilter("rabbitmq", "i", id, "control", "o", lambda x: "1", filter_likes)
middleware.run()