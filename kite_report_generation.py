import json
import os

from collections import OrderedDict
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def flatten(j_elem):
    result = {}
    for key in j_elem.keys():
        if key != 'depth' and key != 'ohlc':
            result[key] = j_elem[key]
    for target in j_elem['depth'].keys():
        for index in range(len(j_elem['depth'][target])):
            for key in j_elem['depth'][target][index]:
                result["depth_{}_{}_{}".format(target, key, index)] = j_elem['depth'][target][index][key]

    for key in j_elem['ohlc']:
        result["{}_{}".format('ohlc', key)] = j_elem['ohlc'][key]
    return OrderedDict(sorted(result.items(), key=lambda t: t[0]))


column_names = ['average_price', 'buy_quantity', 'change', 'depth_buy_orders_0', 'depth_buy_orders_1', 'depth_buy_orders_2',
          'depth_buy_orders_3', 'depth_buy_orders_4', 'depth_buy_price_0', 'depth_buy_price_1', 'depth_buy_price_2',
          'depth_buy_price_3', 'depth_buy_price_4', 'depth_buy_quantity_0', 'depth_buy_quantity_1',
          'depth_buy_quantity_2', 'depth_buy_quantity_3', 'depth_buy_quantity_4', 'depth_sell_orders_0',
          'depth_sell_orders_1', 'depth_sell_orders_2', 'depth_sell_orders_3', 'depth_sell_orders_4',
          'depth_sell_price_0', 'depth_sell_price_1', 'depth_sell_price_2', 'depth_sell_price_3', 'depth_sell_price_4',
          'depth_sell_quantity_0', 'depth_sell_quantity_1', 'depth_sell_quantity_2', 'depth_sell_quantity_3',
          'depth_sell_quantity_4', 'instrument_token', 'last_price', 'last_quantity', 'last_trade_time', 'mode',
          'ohlc_close', 'ohlc_high', 'ohlc_low', 'ohlc_open', 'oi', 'oi_day_high', 'oi_day_low', 'sell_quantity',
          'timestamp', 'tradable', 'volume']


def map_to_csv_line(j_element, separator=","):
    vals = [str(j_element.get(key, "null")) for key in column_names]
    line = separator.join(vals) + "\n"
    return line


if __name__ == '__main__':
    report_folder="/Users/ashutosh.v/Development/bse_data_processing/kite_stream/reports/2019-07-24"
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    conf = SparkConf().setAppName("kite reporting").setMaster('local')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    rdd = sc\
        .textFile("/Users/ashutosh.v/Development/bse_data_processing/kite_stream/stock.log.2019-07-22_08")\
        .flatMap(json.loads)\
        .map(flatten)

    df = rdd.toDF()
    df.cache()

    instruments = [i.instrument_token for i in df.select('instrument_token').distinct().collect()]
    for instrument in instruments:
        df.filter("instrument_token={}".format(instrument)).toPandas().to_csv("{}/{}.csv".format(report_folder, instrument))
