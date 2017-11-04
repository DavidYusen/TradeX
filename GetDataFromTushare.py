import tushare as ts
import pandas as pd
import sqlite3

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

def get_deal_data():
    cf.log_function_start()

    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()
    dbcursor.execute("select distinct(StockCode) from dailytrade where market='SSE Northbound' or market='SZSE Northbound'")
    result = dbcursor.fetchone()

    while result is not None:
        pass

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()
    deal_data = ts.get_k_data()
    print(deal_data)

    cf.log_function_end()
