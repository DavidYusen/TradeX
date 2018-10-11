import logging
import sqlite3

import CommonFunctions as cf
logger = logging.getLogger("TradeX."+__name__)

def create_tables():
    logger.info("Enter create_tables")

    dbconnection = sqlite3.connect('TradeDB.db')
    dbcursor = dbconnection.cursor()

    # create DailyTrade
    dbcursor.execute(
        'create table DailyTrade (Date varchar(20), Market varchar(20), Rank INTEGER, StockCode INTEGER, StockName varchar(20), BuyTurnover INTEGER, SellTurnover INTEGER, TotalTurnOver INTEGER, NetBuyTurnover INTEGER)')
    dbcursor.execute('CREATE UNIQUE INDEX p1 ON DailyTrade(Date, Market, StockCode);')

    dbcursor.close()
    dbconnection.commit()
    dbconnection.close()

    logger.info("Exit create_tables")
