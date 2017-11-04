import logging
import sqlite3

import CommonFunctions as cf

def create_tables():
    cf.log_function_start()

    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    # create DailyTrade
    dbcursor.execute(
        'create table DailyTrade (Date varchar(20), Market varchar(20), Rank INTEGER, StockCode INTEGER, StockName varchar(20), BuyTurnover INTEGER, SellTurnover INTEGER, TotalTurnOver INTEGER, NetBuyTurnover INTEGER)')
    dbcursor.execute('CREATE UNIQUE INDEX p1 ON DailyTrade(Date, Market, StockCode);')

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    cf.log_function_end()
