import json
from datetime import datetime
from enum import Enum
from queue import Queue

from datetime import timedelta

from constants import KITE_EVENT_DATETIME_OBJ, TIMESTAMP
from general_util import csv_file_with_headers_to_json_arr, json_arr_to_csv, flatten
from stock_trade_script import MainClass

EMPTY_KEY = ''

LAST_PRICE = 'last_price'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

VOLUME = 'volume'

string_date_key = 'timestamp'


class MarketEventEmitter:
    def __init__(self, file_name='spicejet.csv'):
        base_path = '../market_analysis_data/csv_files/'

        j_arr = csv_file_with_headers_to_json_arr(base_path + file_name)
        self.__event_list = list(
            map(lambda j_elem: self.__remove_keys(j_elem,
                                                  ['depth', 'Unnamed', 'mode', 'ohlc', 'oi_day',
                                                   'tradable','delta','average_price','oi','change']), j_arr))
        self.__event_iter = iter(self.__event_list)

    @staticmethod
    def __remove_keys(j_elem: dict, text_list_to_remove):
        keys_without_depth = list(
            filter(lambda key: all([text not in key for text in text_list_to_remove]), j_elem.keys()))
        return dict(map(lambda key: (key, j_elem.get(key)), keys_without_depth))

    def emit(self):
        event = next(self.__event_iter)
        event[LAST_PRICE] = float(event[LAST_PRICE])
        event[BUY_QUANTITY] = float(event[BUY_QUANTITY])
        event[SELL_QUANTITY] = float(event[SELL_QUANTITY])
        event[VOLUME] = float(event[VOLUME])
        current_event_time = event[string_date_key]
        dt = datetime.strptime(current_event_time, '%Y-%m-%d %H:%M:%S')
        event[TIMESTAMP] = dt
        return event


if __name__ == '__main__':

    stat_file = "../market_analysis_data/stock_stats/combined_stats.json"
    with open(stat_file) as handle:
        stats = json.load(handle)

    names = list(map(lambda j_elem: j_elem['file_name'], summary_arr))

    summary_arr = csv_file_with_headers_to_json_arr("../market_analysis_data/summary.csv")

    def get_result_time(symbol):
        time_elem = list(filter(lambda j_elem: j_elem['file_name'] == symbol, summary_arr))
        if len(time_elem) != 1:
            raise Exception("Bruh.. the result time is fucked up man.. just look at it: " + str(time_elem))
        return datetime.strptime("{} {}".format(time_elem[0]['date'], time_elem[0]['time_value']), '%Y-%m-%d %H:%M:%S')

    results = {}

    file_names_to_process = ["3MINDIA.csv", "AAVAS.csv"]
    for name in file_names_to_process:
        main_class = MainClass(simulation=True)
        event_emitter = MarketEventEmitter(file_name=name)
        counter = 0
        try:
            while True:
                event = event_emitter.emit()
                main_class.handle_ticks_safely([event])
                counter = counter + 1
        except Exception as e:
            print("caught exception after processing {} lines for file: {}".format(counter, name))

        results[name] = main_class.get_summary()

    with open("/tmp/market_simulation_summary.json", 'w') as handle:
        json.dump(results, handle, indent=2)


