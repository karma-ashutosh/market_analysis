import json
import os
from collections import OrderedDict
from datetime import datetime

import yaml
from pyspark import SparkContext, SparkConf, RDD
from pyspark.sql import SparkSession
import pandas
from bse_util import BseUtil
from connection_factory import get_bse_util, get_kite_util
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


def strip_time(j_elem: dict):
    dt = datetime.strptime(j_elem['timestamp'], '%Y-%m-%d %H:%M:%S')
    j_elem['datetime'] = dt
    j_elem['hour'] = dt.hour
    j_elem['minute'] = dt.minute
    j_elem['date'] = "-".join([str(dt.year).zfill(4), str(dt.month).zfill(2), str(dt.day).zfill(2)])
    j_elem['millis'] = dt.timestamp() * 1000
    return j_elem


def add_grouping_key_and_default_doc_count(j_elem: dict):
    j_elem['stock_minute_grouping_key'] = "{}_{}_{}_{}".format(j_elem.get('instrument_token'),
                                                               j_elem['date'], j_elem['hour'], j_elem['minute'])

    set_millis_to_minute_start_time(j_elem)

    j_elem['count'] = 1
    return j_elem


def set_millis_to_minute_start_time(j_elem):
    j_elem['millis'] = j_elem['millis'] - j_elem['millis'] % 60000


nano_count_dict = {}


def get_unique_nano_from_millis(millis):
    nano = millis * 1000000
    existing_count = nano_count_dict.get(nano, 0)
    nano_count_dict[nano] = existing_count + 1

    new_nano = nano + existing_count
    return new_nano


def to_influx_line(j_elem, additional_tags: dict = None):
    tag_list = ["{}={}".format(name, j_elem.get(name)) for name in tags]

    if additional_tags:
        for key, value in additional_tags.items():
            tag_list.append("{}={}".format(key, value))

    field_list = ["{}={}".format(name, j_elem.get(name)) for name in fields]
    return "kite_web_socket_data,{} {} {}".format(",".join(tag_list), ",".join(field_list),
                                                  "%.0f" % get_unique_nano_from_millis(j_elem.get('millis')))


def get_instrument_to_trading_symbol_mapping():
    return get_kite_util().map_instrument_ids_to_trading_symbol()


def add_trading_symbol(j_elem: dict, instrument_to_symbol_mapping: dict):
    j_elem['trading_symbol'] = instrument_to_symbol_mapping.get(str(j_elem.get('instrument_token')))
    return j_elem


def add_should_log_event_for_partition(j_arr: list) -> None:
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()
    bse_util = BseUtil(config, postgres)

    for j_elem in j_arr:
        j_elem['should_log_event'] = bse_util.should_process_historical_event(j_elem['trading_symbol'],
                                                                              j_elem['datetime'])


def was_event_in_result_announcement_hour(j_elem: dict) -> bool:
    return get_bse_util().should_process_historical_event(j_elem['trading_symbol'], j_elem['datetime'])


influx_file_header = """# DDL
CREATE DATABASE kite_web_socket_data

# DML
# CONTEXT-DATABASE: kite_web_socket_data

"""


def add_grouped_values_and_count(j_elem_1: dict, j_elem_2: dict):
    result = {}
    for key in fields:
        result[key] = j_elem_1[key] + j_elem_2[key]

    for key in tags:
        result[key] = j_elem_1[key]

    result['millis'] = j_elem_1['millis']
    result['count'] = j_elem_1['count'] + j_elem_2['count']
    return result


def convert_summation_to_average(j_elem):
    result = {}
    for key in fields:
        result[key] = (1.0 * j_elem[key]) / j_elem['count']

    for key in tags:
        result[key] = j_elem[key]

    result['millis'] = j_elem['millis']
    return result



def write_all_influx_lines_for_result_hour(processed_rdd: RDD):
    filtered_rdd = processed_rdd.filter(was_event_in_result_announcement_hour)
    save_to_influx_file(filtered_rdd, "{}/influx_lines_result_hour.influx".format(influx_folder), 'per_tick')


def save_to_influx_file(filtered_rdd, file_name, measurement_type):
    influx_lines = filtered_rdd.map(lambda j: to_influx_line(j, {'measurement_type': measurement_type})).collect()
    create_dir_if_not_exists(file_name)
    f = open(file_name, 'w')
    f.write(influx_file_header)
    for line in influx_lines:
        f.write(line + "\n")
    f.flush()
    f.close()


if __name__ == '__main__':
    report_folder = "/Users/ashutosh.v/Development/market_analysis_data/reports"
    source_folder = "/Users/ashutosh.v/Development/market_analysis_data/logs"

    with open('./config.yml') as handle:
        config = yaml.load(handle)

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
        .map(flatten).map(strip_time) \
        .filter(lambda j: 8 < j['hour'] < 16) \
        .map(lambda j: add_trading_symbol(j, instrument_to_sym_mapping))

    rdd.cache()


    df = rdd.toDF().filter("hour > 8").filter("hour < 16").drop('hour')
    df.cache()

    instruments = [i.instrument_token for i in df.select('instrument_token').distinct().collect()]
    for instrument in instruments:
        df.filter("instrument_token={}".format(instrument)).toPandas().to_csv(
            "{}/{}.csv".format(report_folder, instrument))
