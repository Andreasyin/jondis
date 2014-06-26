# -*- coding: utf-8 -*-

import time
from threading import Thread

from nose.tools import assert_raises, eq_
from redis import Connection
from redis import ConnectionError

from jondis.pool import Pool
from jondis.tests.base import BaseJondisTest


class ConnectionPoolTest(BaseJondisTest):

    def start(self):
        self.master = self.manager.start('master')
        self.slave = self.manager.start('slave', self.master)

    def get_pool(self, connection_kwargs=None, max_connections=10,
                 timeout=20):
        hosts = ['127.0.0.1:{}'.format(self.master),
                 '127.0.0.1:{}'.format(self.slave)]
        connection_kwargs = connection_kwargs or {}
        pool = Pool(hosts=hosts,
                    max_connections=max_connections,
                    timeout=timeout,
                    **connection_kwargs)
        return pool

    def test_connection_creation(self):
        connection_kwargs = {'db': 0, 'socket_timeout': 1}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        connection = pool.get_connection('_')
        assert isinstance(connection, Connection)

    def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c2 = pool.get_connection('_')
        assert c1 != c2

    def test_connection_pool_blocks_until_timeout(self):
        """When out of connections, block for timeout seconds, then raise"""
        pool = self.get_pool(max_connections=1, timeout=0.1)
        pool.get_connection('_')

        start = time.time()
        with assert_raises(ConnectionError):
            pool.get_connection('_')
        # we should have waited at least 0.1 seconds
        assert time.time() - start >= 0.1

    def connection_pool_blocks_until_another_connection_released(self):
        """When out of connections, block until another connection is released
        to the pool
        """
        pool = self.get_pool(max_connections=1, timeout=2)
        c1 = pool.get_connection('_')

        def target():
            time.sleep(0.1)
            pool.release(c1)

        Thread(target=target).start()
        start = time.time()
        pool.get_connection('_')
        assert time.time() - start >= 0.1

    def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = pool.get_connection('_')
        c1.connect()
        pool.release(c1)
        c2 = pool.get_connection('_')
        eq_(c1, c2)

    def test_repr(self):
        pool = self.get_pool()
        expected = ("Pool<hosts=set([Server(host='127.0.0.1', "
                    "port={slave}), Server(host='127.0.0.1', "
                    "port={master})]),master=Server(host='127.0.0.1', "
                    "port={master})>".format(slave=self.slave,
                                             master=self.master))
        eq_(repr(pool), expected)
