import json
import os

from collections import OrderedDict
from datetime import datetime

import yaml
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from kite_util import KiteUtil
from postgres_io import PostgresIO


def create_dir_if_not_exists(file_name):
    os.makedirs(os.path.dirname(file_name), exist_ok=True)


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


column_names = ['average_price', 'buy_quantity', 'change', 'depth_buy_orders_0', 'depth_buy_orders_1',
                'depth_buy_orders_2',
                'depth_buy_orders_3', 'depth_buy_orders_4', 'depth_buy_price_0', 'depth_buy_price_1',
                'depth_buy_price_2',
                'depth_buy_price_3', 'depth_buy_price_4', 'depth_buy_quantity_0', 'depth_buy_quantity_1',
                'depth_buy_quantity_2', 'depth_buy_quantity_3', 'depth_buy_quantity_4', 'depth_sell_orders_0',
                'depth_sell_orders_1', 'depth_sell_orders_2', 'depth_sell_orders_3', 'depth_sell_orders_4',
                'depth_sell_price_0', 'depth_sell_price_1', 'depth_sell_price_2', 'depth_sell_price_3',
                'depth_sell_price_4',
                'depth_sell_quantity_0', 'depth_sell_quantity_1', 'depth_sell_quantity_2', 'depth_sell_quantity_3',
                'depth_sell_quantity_4', 'instrument_token', 'last_price', 'last_quantity', 'last_trade_time', 'mode',
                'ohlc_close', 'ohlc_high', 'ohlc_low', 'ohlc_open', 'oi', 'oi_day_high', 'oi_day_low', 'sell_quantity',
                'timestamp', 'tradable', 'volume']

tags = ['trading_symbol', 'date']
fields = ['depth_buy_quantity_0', 'depth_buy_quantity_1', 'depth_buy_quantity_2', 'depth_buy_quantity_3',
          'depth_buy_quantity_4',

          'depth_sell_quantity_0', 'depth_sell_quantity_1', 'depth_sell_quantity_2', 'depth_sell_quantity_3',
          'depth_sell_quantity_4',

          'volume', 'last_price']


def map_to_csv_line(j_element, separator=","):
    vals = [str(j_element.get(key, "null")) for key in column_names]
    line = separator.join(vals) + "\n"
    return line


def strip_time(j_elem):
    dt = datetime.strptime(j_elem['timestamp'], '%Y-%m-%d %H:%M:%S')
    j_elem['hour'] = dt.hour
    j_elem['date'] = "-".join([str(dt.year).zfill(4), str(dt.month).zfill(2), str(dt.day).zfill(2)])
    j_elem['millis'] = dt.timestamp() * 1000
    return j_elem


nano_count_dict = {}


def get_unique_nano_from_millis(millis):
    nano = millis * 1000000
    existing_count = nano_count_dict.get(nano, 0)
    nano_count_dict[nano] = existing_count + 1

    new_nano = nano + existing_count
    return new_nano


def to_influx_line(j_elem):
    tag_list = ["{}={}".format(name, j_elem.get(name)) for name in tags]
    field_list = ["{}={}".format(name, j_elem.get(name)) for name in fields]
    return "kite_web_socket_data,{} {} {}".format(",".join(tag_list), ",".join(field_list),
                                                  "%.0f" % get_unique_nano_from_millis(j_elem.get('millis')))


def get_instrument_to_trading_symbol_mapping():
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()
    k = KiteUtil(postgres, config)
    return k.map_instrument_ids_to_trading_symbol()


def add_trading_symbol(j_elem: dict, instrument_to_symbol_mapping: dict):
    j_elem['trading_symbol'] = instrument_to_symbol_mapping.get(str(j_elem.get('instrument_token')))
    return j_elem


influx_file_header = """# DDL
CREATE DATABASE share_market_data

# DML
# CONTEXT-DATABASE: share_market_data

"""

if __name__ == '__main__':
    source_folder = "/Users/ashutosh.v/Development/bse_data_processing/kite_stream/raw_files/test"
    report_folder = "/Users/ashutosh.v/Development/bse_data_processing/kite_stream/reports/test"
    influx_folder = "/Users/ashutosh.v/Development/bse_data_processing/kite_stream/influx/test"

    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    conf = SparkConf().setAppName("kite reporting").setMaster('local[*]')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    instrument_to_sym_mapping = get_instrument_to_trading_symbol_mapping()

    rdd = sc \
        .textFile("{}/*".format(source_folder)) \
        .repartition(8) \
        .flatMap(json.loads) \
        .map(flatten).map(strip_time)\
        .filter(lambda j: 8 < j['hour'] < 16)\
        .map(lambda j: add_trading_symbol(j, instrument_to_sym_mapping))

    rdd.cache()

    influx_lines = rdd.map(to_influx_line).collect()

    file_name = "{}/influx_lines.influx".format(influx_folder)
    create_dir_if_not_exists(file_name)
    f = open(file_name, 'w')
    f.write(influx_file_header)
    for line in influx_lines:
        f.write(line+"\n")
    f.flush()
    f.close()

    # df = rdd.toDF().filter("hour > 8").filter("hour < 16").drop('hour')
    # df.cache()

    # instruments = [i.instrument_token for i in df.select('instrument_token').distinct().collect()]
    # for instrument in instruments:
    #     df.filter("instrument_token={}".format(instrument)).toPandas().to_csv(
    #         "{}/{}.csv".format(report_folder, instrument))
