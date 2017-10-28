import tushare as ts
import pandas as pd
import sqlite3


def RefreshData():
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