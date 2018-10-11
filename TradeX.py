import logging
from logging.handlers import RotatingFileHandler
import InsertHKTradeDetails as hk
import GetDataFromTushare as tu

logger = logging.getLogger("TradeX")
logger.setLevel(level = logging.DEBUG)

formatter = logging.Formatter('[%(asctime)s] - %(name)s - %(levelname)s - %(message)s')

rHandler = RotatingFileHandler('TradeX.log', maxBytes=1024*1024, backupCount=5)
rHandler.setLevel(logging.INFO)
rHandler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logger.addHandler(rHandler)
logger.addHandler(console)

if __name__ == '__main__':
    logger.info('Main function starts here')

    #更新节假日数据
    # tu.refresh_holiday_data()

    #更新季报数据
    # tu.refresh_season_report_data()

    #更新股票基本信息
    # tu.refresh_stock_basics_data()

    # Insert HGT trade data by period
    hk.insert_data_by_period(20181010, 20181011)

    # Insert HGT trade data by date
    # hk.insert_data_by_date('20180827')

    # Refresh market data
    # tu.refresh_market_data()

    logger.info('Main function ends here')