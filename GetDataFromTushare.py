import tushare as ts
import pandas as pd
import sqlite3
import logging

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