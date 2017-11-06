import tushare as ts
import pandas as pd
import sqlite3
import logging

import CommonFunctions as cf


def RefreshData():
    cf.log_function_start()
    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    trade_date = ts.trade_cal()
    pd.io.sql.to_sql(trade_date, 'Holiday', dbconnection, schema='TradeDB', if_exists='replace')

    stock_basics = ts.get_stock_basics()
    pd.io.sql.to_sql(stock_basics, 'StockBasics', dbconnection, schema='TradeDB', if_exists='replace')

    season_report = ts.get_report_data(2017,3)
    pd.io.sql.to_sql(season_report, 'SeasonReport', dbconnection, schema='TradeDB', if_exists='replace')

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    cf.log_function_end()

def refresh_market_data():
    cf.log_function_start()

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

    cf.log_function_end()