import time
import json
import sqlite3
import logging
from datetime import date
import datetime
from urllib import request

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%d %b %Y %H:%M:%S',
                filename='HKEX.log',
                filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def insert_data_by_date(dealdate):
    url = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_MODE1c.js?MODE2'
    # thedate = time.strftime("%Y%m%d")
    thetimestamp = int(round(time.time() * 1000))
    url = url.replace("MODE1", dealdate).replace('MODE2', str(thetimestamp))
    logging.info('url generated for %s is %s:' % dealdate, url)

    with request.urlopen(url) as f:
        jsondata = f.read().decode('utf-8')[10:]

    textdata = json.loads(jsondata)
    #textdata is a list, each one is a dict



    try:

        dbconnection = sqlite3.connect('TradeDB.db')
        dbcursor = dbconnection.cursor()

        for dictdata in textdata:
            idate = dictdata['date']
            imarket = dictdata['market']
            icontent = dictdata['content']
            for record in icontent[1]['table']['tr']:
                irank = int(record['td'][0][0])
                istockcode = record['td'][0][1]
                istockname = record['td'][0][2]
                ibuyturnover = int(record['td'][0][3].replace(',', ''))
                isellturnover = int(record['td'][0][4].replace(',', ''))
                itotalturnover = int(record['td'][0][5].replace(',', ''))
                inetbuyturnover = ibuyturnover - isellturnover
                t = (idate, imarket, irank, istockcode, istockname, ibuyturnover, isellturnover, itotalturnover, inetbuyturnover)
                dbcursor.execute("insert into DailyTrade VALUES (?,?,?,?,?,?,?,?,?)", t)

        dbcursor.close()
        dbconnection.commit()
        dbconnection.close()

    except Exception as e:
        logging.error('Errors:', e)
    finally:
        logging.info('insert_data_bydate %s is done', dealdate)


def is_deal_date(dealdate):
    dbconnection = sqlite3.connect('hkex.db')
    dbcursor = dbconnection.cursor()

    t = (dealdate,)
    dbcursor.execute('select isOpen from Holiday where calendarDate=?', t)
    result = dbcursor.fetchone()

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logging.debug(dealdate)
    logging.debug(result)
    return result


def insert_data_by_period(istartdate, ienddate):
    tstartdate = datetime.datetime.strptime(str(istartdate), "%Y%m%d")
    startdate = time.strftime("%Y-%m-%d", tstartdate)
    enddate = datetime.datetime.strptime(str(ienddate), "%Y%m%d")
    dealdate = startdate

    while dealdate < enddate:
        logging.info(dealdate)
        if is_deal_date(dealdate):
            insert_data_by_date(dealdate)
        dealdate = dealdate + datetime.timedelta(days=1)


logging.info('TradeX Starts Here.')
insert_data_by_period(20170901, 20170930)
logging.info('TradeX Ends Here.')
