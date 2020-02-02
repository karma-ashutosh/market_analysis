import json
from datetime import datetime

from constants import TIMESTAMP, BASE_DIR, INSTRUMENT_TOKEN
from general_util import csv_file_with_headers_to_json_arr, json_arr_to_csv, flatten
from general_util import map_with_percentage_progress
from stock_trade_script import MainClass
import os

EMPTY_KEY = ''

LAST_PRICE = 'last_price'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

VOLUME = 'volume'

string_date_key = 'timestamp'


class MarketEventEmitter:
    def __init__(self, file_name):
        base_path = BASE_DIR + '/csv_files/'

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
        event[INSTRUMENT_TOKEN] = str(int(float(event[INSTRUMENT_TOKEN])))
        event[LAST_PRICE] = float(event[LAST_PRICE])
        event[BUY_QUANTITY] = float(event[BUY_QUANTITY])
        event[SELL_QUANTITY] = float(event[SELL_QUANTITY])
        event[VOLUME] = float(event[VOLUME])
        current_event_time = event[string_date_key]
        dt = datetime.strptime(current_event_time, '%Y-%m-%d %H:%M:%S')
        event[TIMESTAMP] = dt
        return event


def get_events_from_csv() -> list:
    event_emitter = MarketEventEmitter(file_name=name)
    events = []
    while True:
        try:
            events.append(event_emitter.emit())
        except Exception as e:
            break
    return events


def get_events_from_log_file(file_path: str) -> list:
    j_arr = [json.loads(line.strip()) for line in open(file_path).readlines()]
    result = []
    for tick in j_arr:
        for j_elem in tick:
            strip_time(j_elem)
            result.append(j_elem)
    return result


def strip_time(j_elem: dict):
    dt = datetime.strptime(j_elem['timestamp'], '%Y-%m-%d %H:%M:%S')
    j_elem[TIMESTAMP] = dt


if __name__ == '__main__':

    results = []

    file_names_to_process = os.listdir(BASE_DIR + '/csv_files/')
    for name in ['ADCINDIA.csv']:
        try:
            print(name)
            main_class = MainClass(simulation=True)
            events = get_events_from_log_file("/tmp/kite_logs/kite.log")

            print("Total number of events are: {}".format(len(events)))
            map_with_percentage_progress(events, lambda event: main_class.handle_ticks_safely([event]))
            summary = main_class.get_summary()
            summary = list(summary.values())[0]
            summary['file_name'] = name
            results.append(summary)
        except Exception as e:
            print(e)
            print("Error while processing file: " + name)

    csv_file_path = BASE_DIR + "/simulation_result.csv"
    flat_j_arr = [flatten(j_elem) for j_elem in results]

    for j_elem in flat_j_arr:
        for key in j_elem.keys():
            if isinstance(j_elem[key], datetime):
                j_elem[key] = str(j_elem[key])
    print("saving result to: "+csv_file_path)
    json_arr_to_csv(flat_j_arr, csv_file_path)
