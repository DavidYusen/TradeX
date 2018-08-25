import logging

import InsertHKTradeDetails as hk
import GetDataFromTushare as tu
import CommonFunctions as cf
import DrawPictures as dp

if __name__ == '__main__':
    cf.set_log_format()
    logging.info('Main function starts here')

    # Refresh basic data
    tu.RefreshData()

    # Insert HGT trade data by period
    hk.insert_data_by_period(20180623, 20180731)

    # Insert HGT trade data by date
    # hk.insert_data_by_date('20180326')

    # Refresh market data
    tu.refresh_market_data()

    # dp.DrawCandlestick('600690')


    logging.info('Main function ends here')