import logging
import sqlite3

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%d %b %Y %H:%M:%S',
                filename='TradeX.log',
                filemode='w')

logging.info('CreateTable Starts Here.')

dbconnection = sqlite3.connect('TradeDB.db')
dbcursor = dbconnection.cursor()

# create DailyTrade
dbcursor.execute(
    'create table DailyTrade (Date varchar(20), Market varchar(20), Rank INTEGER, StockCode INTEGER, StockName varchar(20), BuyTurnover INTEGER, SellTurnover INTEGER, TotalTurnOver INTEGER, NetBuyTurnover INTEGER)')
dbcursor.execute('CREATE UNIQUE INDEX p1 ON DailyTrade(Date, Market, StockCode);')

# create Table holiday
dbcursor.execute(
    'CREATE TABLE [Holiday]([calendarDate] DATE, [isOpen] BOOLEAN, [index] INTEGER UNIQUE);')


dbcursor.close()
dbconnection.commit()
dbconnection.close()
