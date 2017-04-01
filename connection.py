# -*- coding:utf-8 -*-

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.packages.hbase import THBaseService
from thrift.packages.hbase.ttypes import *

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090


class Connection(object):

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=None, autoconnect=True):
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.timeout = timeout
        self.transport = None
        self.client = None
        self.refresh_thrift_client()
        if autoconnect:
            self.open()

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

    def get(self, table, key, **kwargs):
        t_get = TGet()
        t_get.row = key
        result = self.client.get(table, t_get)
        data = []
        # 我这里写的真不错～ 嘿嘿
        for columnValue in result.columnValues:
            for (k, v) in kwargs.items():
                if not columnValue.__dict__.get(k) == v:
                    break
            else:
                data.append(columnValue.value)
        return data

    def put(self, table, key, value, family=None, qualifier=None, timestamp=None):
        column = TColumnValue(family=family, qualifier=qualifier, value=value, timestamp=timestamp)
        columns = [column]
        t_put = TPut(key, columns)
        return self.client.put(table, t_put)
