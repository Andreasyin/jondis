# -*- coding: utf-8 -*-

import logging
import os
import threading
from collections import namedtuple
from Queue import Empty, Full, LifoQueue, Queue

from redis import ConnectionError
from redis.client import parse_info
from redis.connection import Connection

Server = namedtuple('Server', ['host', 'port'])


class Pool(object):

    """Thread-safe Redis connection pool

    :param max_connections: max connections in connection pool
    :param timeout: seconds to wait for connection to become available
    """

    def __init__(self, connection_class=Connection, max_connections=50,
                 timeout=20, queue_class=LifoQueue, hosts=None,
                 **connection_kwargs):
        self.pid = os.getpid()
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2 ** 31
        self.timeout = timeout
        self.queue_class = queue_class
        self._origin_hosts = hosts or []
        self._hosts = set()  # current active known hosts
        self._lock = threading.RLock()
        db = self.connection_kwargs.get('db')
        for x in self._origin_hosts:
            if ":" in x:
                (host, port) = x.split(":")
                if "/" in port:
                    port, db = port.split("/")
            else:
                host = x
                port = 6379
            self._hosts.add(Server(host, int(port)))
        self.connection_kwargs['db'] = db or 0

        self.connection_kwargs['socket_timeout'] = self.connection_kwargs.get('socket_timeout') or 1  # noqa

        self._configure()

    def __repr__(self):
        return "{0}<hosts={1},master={2}>".format(type(self).__name__,
                                                  self._hosts,
                                                  self._current_master)

    def _reconfigure(self):
        self.disconnect()
        self.__init__(connection_class=self.connection_class,
                      max_connections=self.max_connections,
                      timeout=self.timeout,
                      hosts=self._origin_hosts, **self.connection_kwargs)

    def _configure(self):
        """Given the servers we know about, find the current master
        once we have the master, find all the slaves.
        """
        self._connections = []
        logging.debug("Running configure")

        self._current_master = None  # (host, port)
        self._master_pool = self.queue_class(self.max_connections)
        self._slave_pool = set()

        with self._lock:
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
                        logging.debug("Current master {0}:{1}".format(
                            x.host, x.port))
                        self._master_pool.put_nowait(conn)
                        slaves = filter(
                            lambda x: x[0:5] == 'slave', info.keys())
                        slaves = [info[y] for y in slaves]
                        if info['redis_version'] >= '2.8':
                            slaves = filter(lambda x: x['state'] == 'online',
                                            slaves)
                            slaves = [Server(s['ip'], int(s['port']))
                                      for s in slaves]
                        else:
                            slaves = [y.split(',') for y in slaves]
                            slaves = filter(lambda x: x[2] == 'online', slaves)
                            slaves = [Server(s[0], int(s[1])) for s in slaves]

                        for y in slaves:
                            if y not in self._hosts:
                                self._hosts.add(y)
                                to_check.put(y)
                except ConnectionError:
                    logging.exception("Can't connect to "
                                      "{host}:{port}/{db}, remove from "
                                      "hosts".format(
                                          host=x.host, port=x.port,
                                          db=self.connection_kwargs.get('db')))
                    self._hosts.discard(x)

            # Fill master connection pool with ``None``
            while True:
                try:
                    self._master_pool.put_nowait(None)
                except Full:
                    break

        logging.debug("Configure complete, host list: {0}".format(self._hosts))

    def _checkpid(self):
        if self.pid != os.getpid():
            self._reconfigure()

    def get_connection(self, command_name, *keys, **options):
        """Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a `ConnectionError`.
        connection = None
        try:
            connection = self._master_pool.get(block=True,
                                               timeout=self.timeout)
            logging.debug("Using connection from pool: %r", connection)
        except Empty:
            if self._current_master:
                raise ConnectionError("No connection available")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            logging.debug("Create new connection")
            connection = self.make_connection()

        return connection

    def make_connection(self):
        """Create a new connection"""
        if self._current_master is None:
            logging.warning("No master set - reconfiguration")
            self._reconfigure()

        if not self._current_master:
            raise ConnectionError("Can't connect to a master")

        host = self._current_master[0]
        port = self._current_master[1]

        logging.info("Creating new connection to {0}:{1}".format(host, port))
        connection = self.connection_class(host=host, port=port,
                                           **self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def release(self, connection):
        """Releases the connection back to the pool. If the connection is dead,
        we disconnect all.
        """
        if connection._sock is None:
            logging.warning("Dead socket, reconfigure")
            self.disconnect()
            self._configure()
            return

        self._checkpid()
        if connection.pid != self.pid:
            logging.debug("Not same procuess: {0} != {1}".format(
                connection.pid, self.pid))
            return

        try:
            self._master_pool.put_nowait(connection)
            logging.debug("Put back connection: {0}".format(connection))
        except Full:
            logging.debug("Master pool is full")
            pass

    def disconnect(self):
        """Disconnects all connections in the pool"""
        for connection in self._connections:
            connection.disconnect()
