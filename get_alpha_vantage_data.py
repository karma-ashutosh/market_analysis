import json
import logging

import requests
import yaml
from alpha_vantage.timeseries import TimeSeries
from enum import Enum
from datetime import timedelta, datetime
from time import sleep
from dateparser import parse
from postgres_io import PostgresIO
from general_util import EventThrottler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
ts = TimeSeries(key='P9F4IQJCUCE6H4C9', output_format='json')

with open('./config.yml') as handle:
    config = yaml.load(handle)
upcoming_results_table = config['alphavantage-config']['upcoming.results.table']
share_price_table = config['alphavantage-config']['share.price.table']
postgres = PostgresIO(config['postgres-config'])
postgres.connect()


class Interval(Enum):
    min1 = "1min"
    min5 = "5min"
    min_15 = "15min"
    min_30 = "30min"
    min_60 = "60min"
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class Exchange(Enum):
    BSE = "BSE"
    NSE = "NSE"


eventThrottler = EventThrottler(window_length_minutes=2, max_event_count_per_window=5)


def get_share_data(symbol: str, exchange: Exchange, interval: Interval) -> list:
    eventThrottler.pauseIfLimitHit(sleep_seconds=60)
    eventThrottler.incrementEventCount(1)

    now = datetime.now()
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}:{}&interval={}" \
          "&apikey=P9F4IQJCUCE6H4C9".format(exchange.value, symbol, interval.value)
    logging.info("getting share data at {} from url {}".format(str(now), url))
    json_data = json.loads(requests.get(url).text)
    keys = json_data.keys()
    meta_data_key = "Meta Data"
    data_key = None
    for key in keys:
        if key != meta_data_key:
            data_key = key
            break
    meta_data, data = json_data.get(meta_data_key), json_data.get(data_key)
    result = []
    for key in data.keys():
        val = data[key]
        val['date'] = str(parse(key) + timedelta(hours=9, minutes=30))
        val['exchange'] = exchange.name
        val['stock_symbol'] = symbol
        val['window'] = interval.name
        val['open'] = val.pop('1. open')
        val['high'] = val.pop('2. high')
        val['low'] = val.pop('3. low')
        val['close'] = val.pop('4. close')
        val['volume'] = val.pop('5. volume')
        result.append(data.get(key))
    return result


if __name__ == '__main__':
    logging.warning("Please don't run this script just before mid night. Some corner cases are not handled well.")
    now = datetime.now()
    if now.hour < 1:
        f_time = datetime(now.year, now.month, now.day, 18)
        delta = f_time - now
        logging.info("current time is {}:{}. Sleeping for {} seconds".format(now.hour, now.minute, delta.seconds))
        sleep(delta.seconds)

    while True:
        now = datetime.now()
        tomorrow_6_pm = datetime(now.year, now.month, now.day, 18) + timedelta(days=1)

        upcoming_results_query = "SELECT * FROM {} WHERE crawling_done = false".format(upcoming_results_table)
        result_list = postgres.execute([upcoming_results_query], fetch_result=True)['result']
        current_time = datetime.now().timestamp()
        for result in result_list:
            try:
                logging.info("details for company under process {}".format(result))
                exchange = result.get('exchange')
                symbol = result.get('symbol')
                hourly_crawling_start_timestamp = float(result.get('hourly_crawling_start_timestamp'))
                hourly_crawling_stop_timestamp = float(result.get('hourly_crawling_stop_timestamp'))
                minute_crawling_start_timestamp = float(result.get('minute_crawling_start_timestamp'))
                minute_crawling_stop_timestamp = float(result.get('minute_crawling_stop_timestamp'))
                share_data_list = []
                flag1, flag2 = False, False

                if float(hourly_crawling_start_timestamp) <= current_time <= float(hourly_crawling_stop_timestamp):
                    flag1 = True
                    logging.debug("getting hourly data")
                    share_data_list.extend(get_share_data(symbol, exchange, Interval.min_60))

                if float(minute_crawling_start_timestamp) <= current_time <= float(minute_crawling_stop_timestamp):
                    flag2 = True
                    logging.debug("getting minute-wise data")
                    share_data_list.extend(get_share_data(symbol, Exchange[exchange], Interval.min1))

                logging.info("inserting in postgres")
                if flag1 or flag2:
                    logging.debug("share_data_list is {}".format(share_data_list))
                    logging.info(postgres.insert_or_skip_on_conflict(share_data_list, share_price_table, ['exchange', 'stock_symbol', 'date', 'window']))
                else:
                    logging.info("marking row as processed for symbol = '{}' and result_date = '{}'"
                                 .format(result.get('symbol'), result.get('result_date')))
                    postgres.execute([
                        "UPDATE {} SET crawling_done = true WHERE symbol = '{}' and result_date = '{}'".format(
                            upcoming_results_table, result.get('symbol'), result.get('result_date')
                        )
                    ], fetch_result=False)
            except:
                logging.exception("Exception occurred while processing: {}".format(result))

        sleep_time = tomorrow_6_pm - datetime.now()
        logging.info("processing done for the day. Sleeping for {} seconds".format(sleep_time.seconds))
        sleep(sleep_time.seconds)




