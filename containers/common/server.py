import socket
import logging
import multiprocessing as mp
import signal
import psutil

class Server:
    def __init__(self, port, listen_backlog, connection_handler):
        # Initialize server socket
        self._open = True
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._connection_handler = connection_handler
        self.next_client_id = 0
        self.connections_queue = mp.Queue()
        
        self._prev_handler = signal.signal(signal.SIGTERM, self.sigterm_handler)

    def run(self):
        """
        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        try:
            while self._open:
                accept_socket = self._accept_new_connection()
                # Envio el socket a la queue como (self.next_client_id, socket)
                # self.next_client_id += 1
                self._connection_handler(accept_socket)
        except socket.error as e:
            if self._open:
                logging.exception(e)
        except Exception as e:
            logging.exception(e)

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


def process_connections(clients_queue):
    should_iterate = True
    should_process_connections = True
    read_connection = clients_queue.get()
    should_iterate = read_connection != None
    while should_iterate:
        if should_process_connection:
            # Llamo a connections_handler
            pass

        read_connection.close()
        read_connection = clients_queue.get()
        should_iterate = read_connection != None

    # Hay que setupear tambien un sigterm handler en el que setee el should_process_connections
    # en false, o podria devolverse en connections_handler si termino por un sigterm y listo, 
    # probablemente sea lo mejor


def process_usage_alternative(desired_processes_amount):
    processes_amount = min(psutil.cpu_count(), desired_processes_amount)
