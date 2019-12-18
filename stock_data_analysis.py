import json
from datetime import datetime
from enum import Enum
from queue import Queue
from general_util import map_with_percentage_progress
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
    def __init__(self, file_name):
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

    results = []

    file_names_to_process = ["3MINDIA.csv"]
    for name in file_names_to_process:
        main_class = MainClass(simulation=True)
        event_emitter = MarketEventEmitter(file_name=name)
        events = []
        while True:
            try:
                events.append(event_emitter.emit())
            except:
                x = 0
                break

        print("Total number of events are: {}".format(len(events)))
        map_with_percentage_progress(events, lambda event: main_class.handle_ticks_safely([event]))
        summary = main_class.get_summary()
        summary['file_name'] = name
        results.append(summary)

    csv_file_path = "../market_analysis_data/simulation_result.csv"
    flat_j_arr = [flatten(j_elem) for j_elem in results]

    for j_elem in flat_j_arr:
        for key in j_elem.keys():
            if isinstance(j_elem[key], datetime):
                j_elem[key] = str(j_elem[key])
    json_arr_to_csv(flat_j_arr, csv_file_path)
