import datetime
import json
import logging
import sqlite3
import time
import pandas as pd
import matplotlib.pyplot as plt

from urllib import request

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%d %b %Y %H:%M:%S',
                    filename='TradeX.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def is_deal_date(dealdate):
    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    t = (dealdate,)
    dbcursor.execute('select isOpen from Holiday where calendarDate=?', t)
    result = dbcursor.fetchone()

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    if result[0] == 1:
        return True
    else:
        return False

def insert_data_by_date(dealdate):
    url = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_MODE1c.js?MODE2'
    # thedate = time.strftime("%Y%m%d")
    thetimestamp = int(round(time.time() * 1000))
    url = url.replace("MODE1", dealdate).replace('MODE2', str(thetimestamp))
    logging.info('url generated for is %s:' % url)

    try:
        with request.urlopen(url) as f:
            jsondata = f.read().decode('utf-8')[10:]

        textdata = json.loads(jsondata)
        # textdata is a list, each one is a dict
    except Exception as e:
        logging.error('Errors:', e)
    finally:
        logging.info('Get data from url for date %s is done', dealdate)


    try:

        dbconnection = sqlite3.connect('TradeDB.db')
        dbcursor = dbconnection.cursor()

        for dictdata in textdata:
            idate = dictdata['date']
            imarket = dictdata['market']
            icontent = dictdata['content']
            for record in icontent[1]['table']['tr']:
                if (record['td'][0][1] != '-'):
                    irank = int(record['td'][0][0])
                    istockcode = record['td'][0][1]
                    istockname = record['td'][0][2]
                    ibuyturnover = int(record['td'][0][3].replace(',', ''))
                    isellturnover = int(record['td'][0][4].replace(',', ''))
                    itotalturnover = int(record['td'][0][5].replace(',', ''))
                    inetbuyturnover = ibuyturnover - isellturnover
                    t = (idate, imarket, irank, istockcode, istockname, ibuyturnover, isellturnover, itotalturnover,
                         inetbuyturnover)
                    dbcursor.execute("insert into DailyTrade VALUES (?,?,?,?,?,?,?,?,?)", t)

        dbcursor.close()
        dbconnection.commit()
        dbconnection.close()

    except Exception as e:
        logging.error('Errors:', e)
    finally:
        logging.info('insert_data_bydate %s is done', dealdate)

def insert_data_by_period(istartdate, ienddate):
    startdate = datetime.datetime.strptime(str(istartdate), "%Y%m%d")
    enddate = datetime.datetime.strptime(str(ienddate), "%Y%m%d")
    dealdate = startdate

    while dealdate < enddate:
        if is_deal_date(dealdate.strftime("%Y-%m-%d")):
            print(dealdate.strftime("%Y%m%d"))
            insert_data_by_date(dealdate.strftime("%Y%m%d"))
        dealdate = dealdate + datetime.timedelta(days=1)


df1 = pd.read_sql("select * from DailyTrade where Market='SSE Northbound'",sqlite3.connect('TradeDB.db'))
df2 = df1.ix[:,[3,8]]
plt.show(df2.plot(kind = 'box'))
#Main Function here
#insert_data_by_period(20170101, 20170731)
# print(is_deal_date('2017-10-22'))
