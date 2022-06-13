from cassandra.cluster import Cluster

cluster=Cluster(['0.0.0.0'], port=9042)

session=cluster.connect()

# create a keyspace, and a table in keyspace
#session.execute("CREATE KEYSPACE IF NOT EXISTS kong_stocks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
#session.execute("CREATE TABLE kong_stocks.ETF_QQQ(OPEN decimal,CLOSE decimal,HIGH decimal,LOW decimal,DATE timestamp,  PRIMARY KEY (DATE));"

import datetime
from datetime import datetime

def unix_time(dt):
    epoch = datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def unix_time_millis(dt):
    return int(unix_time(dt) * 1000.0)

import pytz
# user_tz = pytz.timezone('US/Eastern')
# timestamp_naive = row.ts
# timestamp_utc = pytz.utc.localize(timestamp_naive)
# timestamp_presented = timestamp_utc.astimezone(user_tz)    

session.execute("CREATE TABLE if not exists kong_stocks.ETF_QQQ (OPEN decimal,CLOSE decimal,HIGH decimal,LOW decimal, DATE timestamp, PRIMARY KEY (DATE));")
import yfinance as yf
msft=yf.Ticker('MSFT')
hist=msft.history(period='5d')

hist['Date']=hist.index.map(lambda x: unix_time_millis(x))
#hist['Date']=hist.index.map(lambda x: x.astimezone(user_tz))

print (hist)

print (hist.index[0])

cluster=Cluster(['0.0.0.0'], port=9042)
session = cluster.connect('kong_stocks')
for index,item in hist.iterrows():
       session.execute("INSERT INTO ETF_QQQ (OPEN,CLOSE,HIGH,LOW,DATE) VALUES ({0},{1},{2},{3},{4:.0f});".format(
            item['Open'], item['Close'],item['High'],item['Low'],item['Date']))

k=session.execute (" select * from ETF_QQQ")
column_names=k.one()._fields
print (column_names)

rtn=session.execute("select * from ETF_QQQ")
for r in rtn:
    print (r)

session.execute ("DROP table ETF_QQQ")
