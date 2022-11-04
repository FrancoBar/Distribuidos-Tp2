import socket
import logging
import signal

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
        try:
            while self._open:
                accept_socket = self._accept_new_connection()
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
