from alpha_vantage.timeseries import TimeSeries
from enum import Enum

#
# https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=BSE:TATAMOTORS&interval=1min&apikey=
# P9F4IQJCUCE6H4C9
ts = TimeSeries(key='P9F4IQJCUCE6H4C9', output_format='json')


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


def get_share_data(symbol: str, exchange: Exchange, interval: Interval) -> list:
    data, meta_data = ts.get_intraday(symbol=":".join([exchange.value, symbol]), interval=interval.value, outputsize='full')
    result = []
    for key in data.keys():
        val = data[key]
        val['date'] = key
        val['exchange'] = exchange.name
        val['stock_symbol'] = symbol
        val['window'] = interval.name
        result.append(data.get(key))
    return result
