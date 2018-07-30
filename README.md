#### hbase-thrift2 连接池
支持python2.7, 3.5, 实测可用, 其它不保证

```python

from HBaseByThrift2.connection import Connection as HBaseConn

class hbase_conn(HBaseConn):

    HOST = HBASE_HOST
    PORT = HBASE_PORT

    def __init__(self, host=HOST, port=PORT):
        super(hbase_conn, self).__init__(host=host, port=port)

table = hbase_conn().tableConnection('TableName')
rows = table.gets(rowKeys=rowKeys, filterString=filterString, columns=columns)

for row_key, row in rows:
    yield row
```