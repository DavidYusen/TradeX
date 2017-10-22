import tushare as ts
import pandas as pd
import sqlite3

trade_date = ts.trade_cal()

dbconnection = sqlite3.connect('TradeDB.db')
dbcursor = dbconnection.cursor()

pd.io.sql.to_sql(trade_date, 'Holiday', dbconnection, schema='hkex', if_exists='append')

dbcursor.close()
dbconnection.commit()
dbconnection.close()
