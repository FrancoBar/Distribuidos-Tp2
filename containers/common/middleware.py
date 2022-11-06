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

    def close(self):
        self._open = False
        self._channel.stop_consuming()

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

    def close(self):
        self._open = False
        self._socket.close()

class _BaseFilter:
    def __init__(self, input_queue, output_queue, filter_func):
        self._prev_handler = signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._filter_func = filter_func

    def _on_message_callback(self, input_message):
        output_message = self._filter_func(input_message)
        self._output_queue.send(output_message)

    def send(self, output_message):
        if output_message:
            self._output_queue.send(output_message)

    def put_back(self, output_message):
        if output_message:
            self._input_queue.send(output_message)

    def run(self):
        self._input_queue.start_recv(self._on_message_callback)

    def stop(self):
        self._input_queue.stop_recv()
        self._output_queue.stop_recv()

    def sigterm_handler(self, signum, frame):
        logging.debug('SIGTERM received')
        self._input_queue.close()
        if self._prev_handler:
            self._prev_handler(signum, frame)

class ChannelChannelFilter(_BaseFilter):
    def __init__(self, middleware_host, input_queue, output_queue, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _ChannelQueue(channel, input_queue),
            _ChannelQueue(channel, output_queue),
            filter_func
            )

    def sigterm_handler(self, signum, frame):
        super().sigterm_handler(signum, frame)
        self._connection.close()

class TCPChannelFilter(_BaseFilter):
    def __init__(self, middleware_host, socket, output_queue, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _TCPQueue(socket),
            _ChannelQueue(channel, output_queue),
            filter_func
            )

    def sigterm_handler(self, signum, frame):
        super().sigterm_handler(signum, frame)
        self._connection.close()

class ChannelTCPFilter(_BaseFilter):
    def __init__(self, middleware_host, input_queue, socket, filter_func):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        super().__init__(
            _ChannelQueue(channel, input_queue),
            _TCPQueue(socket),
            filter_func
            )

    def sigterm_handler(self, signum, frame):
        super().sigterm_handler(signum, frame)
        self._connection.close()    

class Adaptor():
    def __init__(self, middleware_host, input_queue, output_queues):
        self._prev_handler = signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=middleware_host))
        channel = self._connection.channel()
        self._input_queue = _ChannelQueue(channel, input_queue)
        self._output_queues = []
        for q in output_queues:
            self._output_queues.append(_ChannelQueue(channel, q))

    def _on_message_callback(self, input_message):
        for q in self._output_queues:
            q.send(input_message)

    def send(self, output_message):
        if output_message:
            for q in self._output_queues:
                q.stop_recv()

    def stop(self):
        self._input_queue.stop_recv()
        for q in self._output_queues:
            q.stop_recv()

    def run(self):
        self._input_queue.start_recv(self._on_message_callback)

    def sigterm_handler(self, signum, frame):
        logging.debug('SIGTERM received')
        self._input_queue.close()
        if self._prev_handler:
            self._prev_handler(signum, frame)
        self._connection.close()
