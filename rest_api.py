import redis
import csv
import calendar
import time
import json

import random
import threading

# response = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=5min&apikey=demo')
# data = response.json()
# print(data['Time Series (5min)'])

r = redis.StrictRedis(host='localhost', port=6379)
r.publish('quote-vm', 'test')


def csv_read_publish(filename, symbol):
    # reading csv file
    with open(filename, 'r') as csvfile:
        # creating a csv reader object
        csv_reader = csv.reader(csvfile)
        csv_reader.__next__()

        for row in csv_reader:
            time.sleep(random.randint(1, 9) * .3)
            timestamp = calendar.timegm(time.strptime(row[0], '%Y-%m-%d %H:%M:%S'))
            quote_data = {'symbol': symbol, 'timestamp': timestamp, 'bid': row[3], 'ask': row[2]}
            quote_data_json = json.dumps(quote_data)
            # print(quote_data_json)
            r.publish('quote-vm', quote_data_json)


msft = threading.Thread(target=csv_read_publish, args=('intraday_1min_MSFT.csv', 'MSFT',))
bac = threading.Thread(target=csv_read_publish, args=('intraday_1min_BAC.csv', 'BAC',))
jpm = threading.Thread(target=csv_read_publish, args=('intraday_1min_JPM.csv', 'JPM',))
wmt = threading.Thread(target=csv_read_publish, args=('intraday_1min_WMT.csv', 'WMT',))

msft.start()
bac.start()
jpm.start()
wmt.start()