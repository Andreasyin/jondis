from Queue import Queue
from itertools import chain
import os
from redis import ConnectionError
from redis.connection import Connection
from redis.client import parse_info
from collections import namedtuple
import logging
import socket

Server = namedtuple('Server', ['host', 'port'])


class Pool(object):

    def __init__(self, connection_class=Connection,
                 max_connections=None, hosts=[],
                 **connection_kwargs):

        self.pid = os.getpid()
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2 ** 31
        self._origin_hosts = hosts
        self._in_use_connections = set()

        self._hosts = set()  # current active known hosts
        self._current_master = None  # (host,port)

        self._master_pool = set()
        self._slave_pool = set()
        self._created_connections = 0

        db = self.connection_kwargs.get('db')
        for x in hosts:
            if ":" in x:
                (host, port) = x.split(":")
                if "/" in port:
                    port, db = port.split("/")
            else:
                host = x
                port = 6379
            host = socket.gethostbyname(host)
            self._hosts.add(Server(host, int(port)))

        self.connection_kwargs['db'] = db or 0
        self._configure()

    def _configure(self):
        """
        given the servers we know about, find the current master
        once we have the master, find all the slaves
        """
        logging.debug("Running configure")
        to_check = Queue()
        for x in self._hosts:
            to_check.put(x)

        while not to_check.empty():
            x = to_check.get()

            try:
                conn = self.connection_class(
                    host=x.host, port=x.port, **self.connection_kwargs)
                conn.send_command("INFO")
                info = parse_info(conn.read_response())

                if info['role'] == 'slave':
                    self._slave_pool.add(conn)
                elif info['role'] == 'master':
                    self._current_master = x
                    logging.debug("Current master {}:{}".format(
                        x.host, x.port))
                    self._master_pool.add(conn)
                    slaves = filter(lambda x: x[0:5] == 'slave', info.keys())
                    slaves = [info[y] for y in slaves]
                    slaves = [y.split(',') for y in slaves]
                    slaves = filter(lambda x: x[2] == 'online', slaves)
                    slaves = [Server(s[0], int(s[1])) for s in slaves]

                    for y in slaves:
                        if y not in self._hosts:
                            self._hosts.add(y)
                            to_check.put(y)

                    # add the slaves
            except:
                # remove from list
                logging.error("Can't connect to: {host}:{port}/{db}".format(host=x.host, port=x.port, db=self.connection_kwargs.get('db')), exc_info=1)
        logging.debug("Configure complete, host list: {}".format(self._hosts))

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.connection_class, self.max_connections,
                          self._origin_hosts, **self.connection_kwargs)

    def get_connection(self, command_name, *keys, **options):
        "Get a connection from the pool"
        self._checkpid()
        try:
            connection = self._master_pool.pop()
            logging.debug("Using connection from pool")
        except KeyError:
            logging.debug("Creating new connection")
            connection = self.make_connection()

        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")

        self._created_connections += 1

        if self._current_master is None:
            logging.warning("No master set - reconfiguratin")
            self._configure()

        if not self._current_master:
            raise ConnectionError("Can't connect to a master")

        host = self._current_master[0]
        port = self._current_master[1]

        logging.info("Creating new connection to {}:{}".format(host, port))
        return self.connection_class(host=host, port=port, **self.connection_kwargs)

    def release(self, connection):

        """
        Releases the connection back to the pool
        if the connection is dead, we disconnect all
        """

        if connection._sock is None:
            logging.warning("Dead socket, reconfigure")
            self.disconnect()
            self._configure()
            self._current_master = None
            server = Server(connection.host, int(connection.port))
            self._hosts.remove(server)
            logging.debug("New configuration: {}".format(self._hosts))

            return

        self._checkpid()
        if connection.pid == self.pid:
            self._in_use_connections.remove(connection)
            self._master_pool.add(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        self._master_pool = set()
        self._slave_pool = set()
        self._in_use_connections = set()
