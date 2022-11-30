import pika
import json
import sys
import os
import socket
import logging
import signal
from asyncio import IncompleteReadError
from .transmition import *

class _ChannelQueue:
    def __init__(self, channel, queue_name):
        self._callback = None
        self._open = True
        self._queue_name = queue_name
        self._channel = channel
        self._channel.queue_declare(queue=queue_name, durable=True)

    def _on_message_callback(self, ch, method, properties, body):
        if not self._open:
            return
        input_message = json.loads(body)
        if self._callback:
            self._callback(input_message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_recv(self, on_message_callback):
        if not self._open:
            return
        self._callback = on_message_callback
        #Helps reducing unfair distribution of work when workload of messages follows a pattern.
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self._on_message_callback)
        try:
            self._channel.start_consuming()
        except IOError as e:
            if self._open:
                raise e
        except Exception as e:
            raise e

    def stop_recv(self):
        self._channel.stop_consuming()

    def send(self, message):
        if message and self._open:
            output_message = json.dumps(message)
            self._channel.basic_publish(
            exchange='',
            routing_key= self._queue_name,
            body=output_message,
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))

    def delete(self):
        self._channel.queue_delete(queue=self._queue_name)

    def close(self):
        self._open = False
        self._channel.stop_consuming()

class _ExchangeQueueIn(_ChannelQueue):
    def __init__(self, channel, input_exchange, input_route_key, control_route_key):
        self._callback = None
        self._open = True
        self._channel = channel
        self._queue_name = input_exchange + '_' + input_route_key
        self._channel.queue_declare(queue=self._queue_name, durable=True)
        self._channel.exchange_declare(exchange=input_exchange, exchange_type='direct', durable=True)
        self._channel.queue_bind(exchange=input_exchange, queue=self._queue_name, routing_key=input_route_key)
        if control_route_key:
            self._channel.queue_bind(exchange=input_exchange, queue=self._queue_name, routing_key=control_route_key)
    
    def send(self, message):
        raise Error("Not implemented for _ExchangeQueueIn")

class _ExchangeQueueOut(_ChannelQueue):
    def __init__(self, channel, output_exchange, output_route_key_gen):
        self._open = True
        self._output_exchange = output_exchange
        self._output_route_key_gen = output_route_key_gen
        self._channel = channel
        self._channel.exchange_declare(exchange=output_exchange, exchange_type='direct', durable=True)

    def send(self, message):
        if message and self._open:
            for route_key in self._output_route_key_gen(message):
                output_message = json.dumps(message)
                self._channel.basic_publish(
                exchange=self._output_exchange,
                routing_key=route_key,
                body=output_message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                ))

    def _on_message_callback(self, ch, method, properties, body):
        raise Error("Not implemented for _ExchangeQueueOut")

    def start_recv(self, on_message_callback):
        raise Error("Not implemented for _ExchangeQueueOut")

    def stop_recv(self):
        raise Error("Not implemented for _ExchangeQueueOut")

class _TCPQueue:
    def __init__(self, socket):
        self._open = True
        self._socket = socket

    def start_recv(self, on_message_callback):
        try:
            while self._open:
                message = json.loads(recv_str(self._socket))
                on_message_callback(message)
        except IncompleteReadError as e:
            if self._open:
                raise e
        except Exception as e:
            raise e

    def stop_recv(self):
        self._open = False

    def send(self, message):
        if message and self._open:
            output_message = json.dumps(message)
            send_str(self._socket, output_message)

    def delete(self):
        pass

    def close(self):
        self._open = False
        self._socket.close()

class _BaseFilter:
    def __init__(self, input_queue, output_queue, filter_func):
        # self._prev_handler = signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._filter_func = filter_func

    def _on_message_callback(self, input_message):
        self._filter_func(input_message)

    def send(self, output_message):
        if output_message:
            self._output_queue.send(output_message)

    def run(self):
        self._input_queue.start_recv(self._on_message_callback)

    def stop(self):
        self._input_queue.stop_recv()

    def delete_input_queue(self):
        self._input_queue.delete()

    def sigterm_handler(self, signum, frame):
        logging.debug('SIGTERM received')
        self._input_queue.close()
        if self._prev_handler:
            self._prev_handler(signum, frame)

class _ConnectionFilter(_BaseFilter):
    def __init__(self, input_queue, output_queue, filter_func):
        super().__init__(input_queue, output_queue,filter_func)
    
    def sigterm_handler(self, signum, frame):
        super().sigterm_handler(signum, frame)
        #self._connection.close()

    def __del__(self):
        self._connection.close()

class ChannelChannelFilter(_ConnectionFilter):
    def __init__(self, middleware_host, input_queue, output_queue, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _ChannelQueue(channel, input_queue),
            _ChannelQueue(channel, output_queue),
            filter_func
            )

class ExchangeExchangeFilter(_ConnectionFilter):
    def __init__(self, middleware_host, input_exchange, input_route_key, control_route_key, output_exchange, output_route_key_gen,  filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _ExchangeQueueIn(channel, input_exchange, input_route_key, control_route_key),
            _ExchangeQueueOut(channel, output_exchange, output_route_key_gen),
            filter_func
            )

class TCPChannelFilter(_ConnectionFilter):
    def __init__(self, middleware_host, socket, output_queue, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _TCPQueue(socket),
            _ChannelQueue(channel, output_queue),
            filter_func
            )

class ChannelTCPFilter(_ConnectionFilter):
    def __init__(self, middleware_host, input_queue, socket, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _ChannelQueue(channel, input_queue),
            _TCPQueue(socket),
            filter_func
            ) 

class TCPExchangeFilter(_ConnectionFilter):
    def __init__(self, middleware_host, socket, output_exchange, output_route_key_gen, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _TCPQueue(socket),
            _ExchangeQueueOut(channel, output_exchange, output_route_key_gen),
            filter_func
            )

class ExchangeTCPFilter(_ConnectionFilter):
    def __init__(self, middleware_host, input_exchange, input_route_key, control_route_key, socket, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _ExchangeQueueIn(channel, input_exchange, input_route_key, control_route_key),
            _TCPQueue(socket),
            filter_func
            )
