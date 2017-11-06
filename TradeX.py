import logging

import InsertHKTradeDetails as hk
import GetDataFromTushare as tu
import CommonFunctions as cf

if __name__ == '__main__':
    cf.set_log_format()
    logging.info('Main function starts here')

    # Refresh basic data
    # tu.RefreshData()

    # Insert HGT trade data by period
    # hk.insert_data_by_period(20171027, 20171104)

    # Insert HGT trade data by date
    # hk.insert_data_by_date('20171106')

    # Refresh market data
    tu.refresh_market_data()


    logging.info('Main function ends here')