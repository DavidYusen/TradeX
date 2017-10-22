#This is a test for http://python.jobbole.com/87813/?utm_source=blog.jobbole.com&utm_medium=relatedPosts

import pandas as pd
import pandas.io.data as web
import datetime

start = datetime.datetime(2016,1,1)
end = datetime.date.today()

apple = web.DataReader("AAPL", "yahoo", start, end)

type(apple)