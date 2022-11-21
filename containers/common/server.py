import socket
import logging
import multiprocessing as mp
import signal
import psutil
from common import utils

config = utils.initialize_config()
LOGGING_LEVEL = config['GENERAL']['logging_level']
utils.initialize_log(LOGGING_LEVEL)

MAX_DESIRED_CONNECTIONS = int(config['SERVER']['max_desired_connections'])

class BooleanSigterm:
    def __init__(self):
        self.should_keep_processing = True
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def handle_sigterm(self):
        self.should_keep_processing = False

class Server:
    def __init__(self, port, listen_backlog, connection_handler):
        # Initialize server socket
        self._open = True
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._connection_handler = connection_handler
        
        self._prev_handler = signal.signal(signal.SIGTERM, self.sigterm_handler)

    def run(self):
        """
        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        connections_queue = mp.Queue()
        next_client_number = 0
        processes_amount = max(psutil.cpu_count(), MAX_DESIRED_CONNECTIONS)
        child_processes = []
        for _ in range(processes_amount):
            p = mp.Process(target=self.process_connections, args=[connections_queue])
            p.start()
            child_processes.append(p)

        try:
            while self._open:
                accept_socket = self._accept_new_connection()
                # Envio el socket a la queue como (self.next_client_id, socket)
                connections_queue.put((accept_socket, next_client_number))
                next_client_number += 1
                # self._connection_handler(accept_socket)
        except socket.error as e:
            if self._open:
                logging.exception(e)
        except Exception as e:
            logging.exception(e)

        for _ in range(len(child_processes)):
            connections_queue.put(None)
        connections_queue.close()
        for child_process in child_processes:
            child_process.terminate()
        for child_process in child_processes:
            child_process.join()
        connections_queue.join_thread()

    def _accept_new_connection(self):
        """
        Accept new connections
        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        logging.info("Proceed to accept new connections")
        c, addr = self._server_socket.accept()
        logging.info('Got connection from {}'.format(addr))
        return c

    def sigterm_handler(self, signum, frame):
        logging.debug('SIGTERM received')
        self._open = False
        self._server_socket.close()
        if self._prev_handler:
            self._prev_handler(signum, frame)

    def process_connections(self, clients_queue):
        read_connection = clients_queue.get()
        boolean_sigterm = BooleanSigterm()
        while read_connection != None:
            accept_socket, next_client_number = read_connection
            if boolean_sigterm.should_keep_processing:
                self._connection_handler(accept_socket, f'client_{next_client_number}')
            read_connection.close()
            read_connection = clients_queue.get()

