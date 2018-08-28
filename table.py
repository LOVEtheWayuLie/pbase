# -*- coding:utf-8 -*-
from .thrift2.hbase.ttypes import *
from abc import ABCMeta, abstractmethod
from .thrift2.utils import make_to_dict, make_ordered_to_dict
from collections import Iterable
import six


class TableInterface( six.with_metaclass( ABCMeta)):
    '''
    table operating
    All operations are sorted by timestamp
    Timestamp The latest record will take effect, such as deleting the put and so on
    '''

    @abstractmethod
    def __del__(self):
        pass

    @abstractmethod
    def get(self, rowKey, includeTimestamp=False):
        pass

    @abstractmethod
    def gets(self, rowKeys, columns=None, filterString=None):
        pass

    @abstractmethod
    def put(self, rowKey, data, timestamp=None, writeToWal=True):
        '''
        :return:  The input and query result formats are consistent
        '''
        pass

    @abstractmethod
    def delete(self, rowKey, columns=None, timestamp=None, writeToWal=True):
        '''
        :param rowKey:
        :param columns: ['family:qualifier','...'] or [('family:qualifier',timestamp),'...']
        :param timestamp:
        :param writeToWal:
        :return:
        '''
        pass

    @abstractmethod
    def scan(self, limit=None, startRow=None, stopRow=None, columns=None, timeRange=None, filterString=None, caching=None, batchSize=1000, includeTimestamp=False, attributes=None, maxVersions=1):
        '''
        :param limit:
        :param startRow:
        :param stopRow:
        :param columns:
        :param timeRange:
        :param filterString:
        :param caching:
        :param batchSize:
        :param includeTimestamp:
        :param attributes:
        :param maxVersions:
        :return:
        '''
        pass



class Table(TableInterface):

    def __init__(self, tableName, connection):
        self.__tableName = tableName
        self.connection = connection
        self.client = connection.client

    def __repr__(self):
        return '<%s.%s tableName=%r>' % (
            __name__,
            self.__class__.__name__,
            self.__tableName,
        )

    def close(self):
        self.connection.close()

    def __del__(self):
        self.connection.close()

    def get(self, rowKey, columns=None, filterString=None, includeTimestamp=False):
        if columns is not None:
            t_column = []
            t_column.extend(
                TColumn(
                    *( family_qualifier.split(':') )
                )
                for family_qualifier in columns
            )
        else: t_column = None
        t_get = TGet(
            row=rowKey,
            columns=t_column,
            filterString=filterString
        )
        item = self.client.get(self.__tableName, t_get)
        row = make_to_dict(item.columnValues, includeTimestamp)
        return item.row,row


    def gets(self, rowKeys, columns=None, filterString=None, include_timestamp=False):
        if not isinstance(rowKeys, Iterable):
            raise ValueError('rowKeys Can not iterate')
        if columns is not None:
            t_column = []
            t_column.extend(
                TColumn(
                    *( family_qualifier.split(':') )
                )
                for family_qualifier in columns
            )
        else: t_column = None
        t_gets = []
        t_gets.extend(
            TGet(
                row=rowKey,
                columns=t_column,
                filterString=filterString
            )
            for rowKey in rowKeys
        )
        items = self.client.getMultiple(self.__tableName,t_gets)
        for item in items:
            row = make_to_dict(item.columnValues, include_timestamp)
            yield item.row, row


    def getTPut(self, rowKey, data, timestamp, writeToWal):
        if data is None:
            raise ValueError("data can not be None,Format reference query results")
        columns = []
        columns.extend(
            TColumnValue(
                *( family_qualifier.split(':') + list(value_timestamp if isinstance(value_timestamp,tuple) else (value_timestamp,timestamp)) )
            )
            for family_qualifier,value_timestamp in data.items()
        )
        t_put = TPut(
            row=rowKey,
            columnValues=columns,
            timestamp=timestamp, writeToWal=writeToWal
        )
        return t_put


    def put(self, rowKey, data, timestamp=None, writeToWal=True):
        t_put = self.getTPut(rowKey, data, timestamp, writeToWal)
        # tag storyofus
        # self.client ====>  thrift2.hbase.THBaseService.Client
        return self.client.put(self.__tableName, t_put)


    def puts(self, puts, timestamp=None, writeToWal=True):
        if not isinstance(puts, Iterable):
            raise ValueError("puts Can not iterate")
        t_puts = []
        t_puts.extend(
            self.getTPut(rowKey, data, timestamp=timestamp, writeToWal=writeToWal)
            for rowKey, data in puts
        )
        return self.client.putMultiple(self.__tableName, t_puts)


    def delete(self, rowKey, columns=None, timestamp=None, writeToWal=True):
        if columns is not None:
            t_columns_list = []
            t_columns_list.extend(
                TColumn(
                    *( (family_qualifier.split(':') + [timestamp]) if not isinstance(family_qualifier,tuple) else (family_qualifier[0].split(':') + [family_qualifier[1]]) )
                )
                for family_qualifier in columns
            )
        else: t_columns_list = None

        t_delete = TDelete(
            row=rowKey, columns=t_columns_list,
            timestamp=timestamp, deleteType=1,
            writeToWal=writeToWal, attributes=None,
            durability=None
        )
        return self.client.deleteSingle(self.__tableName, t_delete)


    def deletes(self, rowKeys, columns=None, timestamp=None, writeToWal=True):
        if not isinstance(rowKeys,Iterable):
            raise ValueError('rowKeys Can not iterate')
        if columns is not None:
            t_column = []
            t_column.extend(
                TColumn(
                    *( family_qualifier.split(':') )
                )
                for family_qualifier in columns
            )
        else: t_column = None
        t_deletes = []
        t_deletes.extend(
            TDelete(
                row=rowKey,
                columns=t_column,
                timestamp=timestamp,
                writeToWal=writeToWal
            )
            for rowKey in rowKeys
        )
        return self.client.deleteMultiple(self.__tableName, t_deletes)


    def scan(self, limit=None, startRow=None, stopRow=None, columns=None, timeRange=None, filterString=None, caching=1000, batchSize=None, includeTimestamp=False, attributes=None, maxVersions=1):

        if limit is not None and limit < 1:
            raise ValueError("'limit' must be >= 1")
        if caching is not None and caching < 1:
            raise ValueError("'caching' must be >= 1")

        if columns is not None:
            t_column = []
            t_column.extend(
                TColumn(
                    *( family_qualifier.split(':') )
                )
                for family_qualifier in columns
            )
        else: t_column = None
        timeRange = timeRange and TTimeRange(*timeRange)
        t_scan = TScan(
            startRow=startRow, stopRow=stopRow,
            columns=t_column, timeRange=timeRange,
            filterString=filterString,
            caching=caching, batchSize=batchSize,
            attributes=attributes,maxVersions=maxVersions,
        )
        scannerId = self.client.openScanner(self.__tableName, t_scan)
        n_returned = n_fetched = 0
        try:
            while True:
                if limit is None:
                    how_many = caching
                else:
                    how_many = min(caching, limit - n_returned)

                items = self.client.getScannerRows(scannerId, how_many)

                if not items:
                    return   # scan has finished

                n_fetched += len(items)

                for n_returned, item in enumerate(items,n_returned + 1):
                    row = make_to_dict(item.columnValues, includeTimestamp)
                    yield item.row, row
                    if limit is not None and n_returned == limit:
                        return  # scan has finished
        except:
            import traceback
            traceback.print_exc()
        finally:
            self.client.closeScanner(scannerId)