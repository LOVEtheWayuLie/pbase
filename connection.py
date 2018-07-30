# -*- coding:utf-8 -*-
from abc import ABCMeta, abstractmethod

from .table import Table
from .thrift2.hbase import *
from .thrift2.protocol import TBinaryProtocol
from .thrift2.transport import TSocket
from .thrift2.transport import TTransport

class ConnIFace:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __del__(self):
        pass

    @abstractmethod
    def refresh_thrift_client(self):
        pass

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def tableConnection(self, tableName):
        pass


class Connection(ConnIFace):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 9090

    def __init__(self, host=None, port=None, timeout=None, autoconnect=True):
        self.host = host or self.__class__.DEFAULT_HOST
        self.port = port or self.__class__.DEFAULT_PORT
        self.timeout = timeout
        self.transport = None
        self.client = None
        self.__tableName = None
        self.refresh_thrift_client()
        if autoconnect:
            self.open()

    def __repr__(self):
        return '<%s.%s host=%r,port=%r>' % (
            __name__,
            self.__class__.__name__,
            self.host,
            self.port,
        )

    def __del__(self):
        self.close()

    def refresh_thrift_client(self):
        socket = TSocket.TSocket(self.host, self.port)

        if self.timeout:
            socket.setTimeout(self.timeout * 1000)

        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = THBaseService.Client(protocol)
        self.transport = transport
        self.client = client

    def open(self):
        if self.transport.isOpen():
            return
        self.transport.open()

    def close(self):
        if not self.transport.isOpen():
            return
        self.transport.close()

    def tableConnection(self, tableName):
        self.__tableName = tableName
        return Table(tableName,self)