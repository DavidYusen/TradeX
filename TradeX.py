import logging
import InsertHgtTradeDetails as hgt
import GetDataFromTushare as tu


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%d %b %Y %H:%M:%S',
                    filename='TradeX.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# df1 = pd.read_sql("select * from DailyTrade where Market='SSE Northbound'",sqlite3.connect('TradeDB.db'))
# df2 = df1.ix[:,[3,8]]
# plt.show(df2.plot(kind = 'box'))
# Main Function here
# hgt.insert_data_by_date('20171025')
# insert_data_by_period(20171023, 20171024)
# print(is_deal_date('2017-10-22'))
# tu.RefreshData()