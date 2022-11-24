import random
from .transmition import ExchangeExchangeFilter as BaseExchangeExchangeFilter,TCPExchangeFilter,ExchangeTCPFilter

class PoisonedExchangeExchangeFilter(ExchangeExchangeFilter):
    def __init__(self, middleware_host, input_exchange, input_route_key, control_route_key, output_exchange, output_route_key_gen,  filter_func):
    	self.duplication_prob = 1.0
    	super.__init__(middleware_host, input_exchange, input_route_key, control_route_key, output_exchange, output_route_key_gen,  filter_func)

    def send(self, output_message):
        if output_message:
            self._output_queue.send(output_message)
            if random.random() <= self.duplication_prob:
            	self._output_queue.send(output_message)

    def set_duplication_prob(self, duplication_prob):
    	self.duplication_prob = duplication_prob

class PoisonedTCPExchangeFilter(TCPExchangeFilter):
    def __init__(self, middleware_host, socket, output_exchange, output_route_key_gen, filter_func):
    	self.duplication_prob = 1.0
    	super.__init__(middleware_host, socket, output_exchange, output_route_key_gen, filter_func)

    def send(self, output_message):
        if output_message:
            self._output_queue.send(output_message)
            if random.random() <= self.duplication_prob:
            	self._output_queue.send(output_message)

    def set_duplication_prob(self, duplication_prob):
    	self.duplication_prob = duplication_prob

class PoisonedExchangeTCPFilter(ExchangeTCPFilter):
    def __init__(self, middleware_host, input_exchange, input_route_key, control_route_key, socket, filter_func):
        self.duplication_prob = 1.0
        super.__init__(middleware_host, input_exchange, input_route_key, control_route_key, socket, filter_func)

    def send(self, output_message):
        if output_message:
            self._output_queue.send(output_message)
            if random.random() <= self.duplication_prob:
            	self._output_queue.send(output_message)

    def set_duplication_prob(self, duplication_prob):
    	self.duplication_prob = duplication_prob
