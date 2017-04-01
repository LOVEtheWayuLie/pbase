# -*- coding:utf-8 -*-

import contextlib
import logging
import socket
import threading
import Queue

from thriftpy.thrift import TException
from connection import Connection

logger = logging.getLogger(__name__)


class NoConnectionsAvailable(RuntimeError):
    pass


class ConnectionPool(object):
    def __init__(self, size, **kwargs):
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(
            "Initializing connection pool with %d connections", size)

        self._lock = threading.Lock()
        self._queue = Queue.LifoQueue(maxsize=size)
        self._thread_connections = threading.local()
        connection_kwargs = kwargs
        connection_kwargs['autoconnect'] = False

        for i in range(size):
            connection = Connection(**connection_kwargs)
            self._queue.put(connection)

        with self.connection():
            pass

    def _acquire_connection(self, timeout=None):
        try:
            return self._queue.get(True, timeout)
        except Queue.Empty:
            raise NoConnectionsAvailable(
                "No connection available from pool within specified "
                "timeout")

    def _return_connection(self, connection):
        self._queue.put(connection)

    @contextlib.contextmanager
    def connection(self, timeout=None):
        connection = getattr(self._thread_connections, 'current', None)

        return_after_use = False
        if connection is None:
            return_after_use = True
            connection = self._acquire_connection(timeout)
            with self._lock:
                self._thread_connections.current = connection

        try:
            connection.open()
            yield connection

        except (TException, socket.error):
            logger.info("Replacing tainted pool connection")
            connection.close()
            connection.refresh_thrift_client()
            connection.open()

        finally:
            if return_after_use:
                del self._thread_connections.current
                self._return_connection(connection)

    # 在保证没有其他操作之后，关闭池内所有连接
    def close_all(self):
        while not self._queue.empty():
            connection = self._queue.get(True)
            connection.close()
