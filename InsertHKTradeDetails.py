import datetime
import json
import logging
import sqlite3
import time
from urllib import request

logger = logging.getLogger("TradeX."+"__name__")

def is_deal_date(dealdate):
    logger.info("Enter is_deal_date")

    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    t = (dealdate,)
    dbcursor.execute('select isOpen from Holiday where calendarDate=?', t)
    result = dbcursor.fetchone()

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logger.info("Exit is_deal_date")

    if result[0] == 1:
        return True
    else:
        return False

def insert_data_by_date(dealdate):
    logger.info("Enter insert_data_by_date")

    url = 'http://sc.hkex.com.hk/TuniS/www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_MODE1c.js?MODE2'
    # thedate = time.strftime("%Y%m%d")
    thetimestamp = int(round(time.time() * 1000))
    url = url.replace("MODE1", dealdate).replace('MODE2', str(thetimestamp))
    logger.debug('url generated for is %s:' % url)

    try:
        with request.urlopen(url) as f:
            jsondata = f.read().decode('utf-8')[10:]

        textdata = json.loads(jsondata)
        # textdata is a list, each one is a dict
    except Exception as e:
        logger.error('Errors:', e)
    finally:
        logger.info('Get data from url for date %s is done', dealdate)


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
        logger.error('Errors:', e)
    finally:
        logger.info('Insert HK trade data for date %s is done', dealdate)
        logger.info("Exit insert_data_by_date")

def insert_data_by_period(istartdate, ienddate):
    logger.info("Enter insert_data_by_period")

    startdate = datetime.datetime.strptime(str(istartdate), "%Y%m%d")
    enddate = datetime.datetime.strptime(str(ienddate), "%Y%m%d")
    dealdate = startdate

    while dealdate < enddate:
        if is_deal_date(dealdate.strftime("%Y-%m-%d")):
            insert_data_by_date(dealdate.strftime("%Y%m%d"))
        dealdate = dealdate + datetime.timedelta(days=1)

    logger.info("Exit insert_data_by_period")
