import tushare as ts
import pandas as pd
import sqlite3
import logging
import time

ts.set_token('96f42c2df864db5b2b5dabe8ef76a623e05f8a5c39fd6b8c2c72e004')

logger = logging.getLogger("TradeX."+__name__)

def refresh_holiday_data():
    logger.debug("Enter refresh_holiday_data")
    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    trade_date = ts.trade_cal()
    pd.io.sql.to_sql(trade_date, 'Holiday', dbconnection, schema='TradeDB', if_exists='replace')
    logger.info("refresh holiday data successfully in TradeDB")

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logger.debug("Exit refresh_holiday_data")


def refresh_stock_basics_data():
    logger.debug("Enter refresh_stock_basics_data")
    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    stock_basics = ts.get_stock_basics()
    pd.io.sql.to_sql(stock_basics, 'StockBasics', dbconnection, schema='TradeDB', if_exists='replace')
    logger.info("refresh refresh_stock_basics_data successfully in TradeDB")

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logger.debug("Exit refresh_stock_basics_data")


def refresh_season_report_data():
    logger.debug("Enter refresh_season_report_data")
    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    season_report = ts.get_report_data(2018,1)
    pd.io.sql.to_sql(season_report, 'SeasonReport', dbconnection, schema='TradeDB', if_exists='replace')
    logger.info("refresh refresh_season_report_data successfully in TradeDB")

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logger.debug("Exit refresh_season_report_data")


def refresh_market_data():
    logger.info("Enter refresh_market_data")

    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    dbcursor.execute(
        "select distinct(StockCode) from dailytrade where market='SSE Northbound' or market='SZSE Northbound'")
    results = dbcursor.fetchall()

    if results is not None:
        for result in results:
            stockcode = str(result[0])
            while len(stockcode) < 6:
                stockcode = '0' + stockcode

            try:
                market_data = ts.get_k_data(stockcode, start='2017-06-01',)
                pd.io.sql.to_sql(market_data, 'MarketData', dbconnection, schema='TradeDB', if_exists='append')
            except Exception as e:
                logging.error('Errors:', e)
            finally:
                logging.info('Get %s market data is done', stockcode)

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logger.info("Exit refresh_market_data")


def insert_pro_daily_by_period(istartdate, isenddate):
    startdate = str(istartdate)
    logger.info('startdate: %s', startdate)

    enddate = str(isenddate)
    logger.info('enddate: %s', enddate)

    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    dbcursor.execute("select distinct(StockCode) from dailytrade where market='SSE Northbound'")
    results = dbcursor.fetchall()

    if results is not None:
        for result in results:
            stockcode = str(result[0])
            while len(stockcode) < 6:
                stockcode = '0' + stockcode
            stockcode = stockcode + '.SH'

            try:
                pro = ts.pro_api()
                print(stockcode, startdate, enddate)
                time.sleep(1)
                df = pro.daily(ts_code=stockcode, start_date=startdate, end_date=enddate)
                print(df)
                pd.io.sql.to_sql(df, 'ProDailyTrade', dbconnection, schema='TradeDB', if_exists='append')
            except Exception as e:
                logger.error('Errors:', e)
            finally:
                logger.info('Get %s Daily data is done', stockcode)

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()


def testma():
    logger.info('testma started')
    pro = ts.pro_api()
    df = pro.daily(ts_code='600036.SH', start_date='20190101', end_date='20190828')

    lamount3 = lamount5 = lamount10 = lamount = df.amount * 1000
    lvol3 = lvol5 = lvol10 = lvol = df.vol * 100
    for i in range(len(lamount)):
        lamount10[i] = listsum(lamount, i, 10)
        lvol10[i] = listsum(lvol, i, 10)
        lamount5[i] = listsum(lamount, i, 5)
        lvol5[i] = listsum(lvol, i, 5)
        lamount3[i] = listsum(lamount, i, 3)
        lvol3[i] = listsum(lvol, i, 3)

    lma10 = lamount10/lvol10
    lma5 = lamount5/lvol5
    lma3 = lamount3/lvol3
    # df.ma10 = lma10
    # df.ma5 = lma5
    # df.ma3 = lma3
    # df = pd.concat([df, pd.DataFrame(columns=lma3)])
    # df = pd.concat([df, pd.DataFrame(columns=lma5)])
    # df = pd.concat([df, pd.DataFrame(columns=lma10)])
    # print(df)
    print(lma10, lma5, lma3)
    logger.info('testma finished')

def listsum(lamount, i, n):
    m = min(len(lamount), i+n)
    result = 0
    while i < m:
        result = result + lamount[i]
        i = i+1
    return result

