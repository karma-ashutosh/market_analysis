import json
from datetime import datetime
from enum import Enum
from queue import Queue

from datetime import timedelta

from general_util import csv_file_with_headers_to_json_arr, json_file_to_csv

EMPTY_KEY = ''

LAST_PRICE = 'last_price'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

VOLUME = 'volume'


class CountBasedEventWindow:
    def __init__(self, number_of_events_in_window: int, keys_to_track: list):
        self.__window_length = number_of_events_in_window
        self.__keys_to_track = keys_to_track

        self._events = Queue(maxsize=number_of_events_in_window)
        self.__initialize_events_queue()

        self._sum = [0] * len(keys_to_track)

    def __initialize_events_queue(self):
        d = {}
        for key in self.__keys_to_track:
            d[key] = 0
        for _ in range(self.__window_length):
            self._events.put_nowait(d)

    def move(self, json_event: dict):
        out_of_window_event = self._events.get_nowait()
        for i in range(len(self.__keys_to_track)):
            key = self.__keys_to_track[i]
            self._sum[i] = self._sum[i] - float(out_of_window_event.get(key, 0)) + float(json_event.get(key, 0))
        self._events.put_nowait(json_event)

    def get_avg(self, key=None):
        if key:
            key_pos = self.__keys_to_track.index(key)
            return self._sum[key_pos] / self.__window_length
        else:
            return [elem / self.__window_length for elem in self._sum]


class MarketEventEmitter:
    def __init__(self, file_name='spicejet.csv'):
        base_path = '../market_analysis_data/csv_files/'

        j_arr = csv_file_with_headers_to_json_arr(base_path + file_name)
        self.__event_list = list(
            map(lambda j_elem: self.__remove_keys(j_elem,
                                                  ['depth', 'Unnamed', 'instrument_token', 'mode', 'ohlc', 'oi_day',
                                                   'tradable']), j_arr))
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
        return event


class MarketChangeDetector:
    def __init__(self, event_emitter: MarketEventEmitter, string_date_key, window_len, base_filter_func, long_score_func_list: list,
                 short_score_func_list: list, score_filter_func, execute_entry_func, execute_exit_func):
        self._event_emitter = event_emitter
        self._base_filter_func = base_filter_func
        self._long_score_func_list = long_score_func_list
        self._short_score_func_list = short_score_func_list
        self._score_filter_func = score_filter_func
        self._execute_entry_func = execute_entry_func
        self._execute_exit_func = execute_exit_func

        self._string_date_key = string_date_key

        keys_to_track = [EMPTY_KEY, self._string_date_key, VOLUME, BUY_QUANTITY, SELL_QUANTITY, LAST_TRADE_TIME, LAST_PRICE]
        self._event_window_15_sec = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                                keys_to_track=keys_to_track,
                                                                string_date_key=self._string_date_key)

        self._filter_pass_key_name = 'bool'
        self._filter_pass_queue = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                              keys_to_track=[self._filter_pass_key_name],
                                                              string_date_key=self._string_date_key)

        self._took_position = False

    def run(self):
        while True:
            try:
                market_event = self._event_emitter.emit()
                if not self._took_position:
                    self._try_take_position(market_event)
                else:
                    if self._execute_exit_func(market_event[LAST_PRICE]):
                        # print("Done for today. Have booked millions of $$$. Enjoy brother")
                        break

            except StopIteration as e:
                print(e)
                # print("Out of events to emit. Market closes. Go smoke all that money")
                break

    def _try_take_position(self, market_event):
        def set_flag_with_base_filter_func():
            if self._base_filter_func(current_event_list_view):
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            else:
                self._filter_pass_queue.move(self.get_filter_event(market_event, False))

        self._event_window_15_sec.move(market_event)
        current_event_list_view = self._event_window_15_sec.get_current_queue_snapshot()
        if any([e[self._filter_pass_key_name] for e in self._filter_pass_queue.get_current_queue_snapshot()]):
            if current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].hour == 14 \
                    and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].minute == 59 \
                    and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].second > 10:
                x = 0
            long_scores = list(map(lambda func: func(current_event_list_view), self._long_score_func_list))
            short_scores = list(map(lambda func: func(current_event_list_view), self._short_score_func_list))
            if self._score_filter_func(long_scores):
                self._execute_entry_func(current_event_list_view, TransactionType.LONG)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
                self._took_position = True
            elif self._score_filter_func(short_scores):
                self._execute_entry_func(current_event_list_view, TransactionType.SHORT)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
                self._took_position = True
            else:
                set_flag_with_base_filter_func()

        else:
            set_flag_with_base_filter_func()

    @staticmethod
    def debug_point(current_event_list_view):
        if current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].hour == 14 \
                and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].minute == 54 \
                and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].second == 7:
            x = 0

    def get_filter_event(self, market_event, state: bool):
        filter_event = {
            self._string_date_key: market_event.get(self._string_date_key),
            self._filter_pass_key_name: state
        }
        return filter_event


class PerSecondLatestEventTracker:
    DATETIME_OBJ = 'datetime'

    def __init__(self, window_length_in_seconds: int, keys_to_track: list, string_date_key: str):
        self.__window_length = window_length_in_seconds
        self.__keys_to_track = keys_to_track
        self.__string_date_key = string_date_key

        self.__events = Queue(maxsize=window_length_in_seconds)
        self._sum = [0] * len(keys_to_track)

    def get_current_queue_snapshot(self) -> list:
        return list(self.__events.queue)

    def move(self, json_event: dict):
        current_event = self.__get_marshaled_event(json_event)

        queue_as_list = self.__events.queue
        if len(queue_as_list) > 0:
            last_element = queue_as_list[-1]
            seconds_gap = self.__get_seconds_gap_from_last_received_event(last_element, current_event)
            # print(seconds_gap, 'gap')

            if seconds_gap < 1:
                self.__overwrite_event(last_element, current_event)
            else:
                for i in range(seconds_gap - 1):
                    self.__put(last_element)

            self.__put(current_event)
        else:
            self.__put(current_event)

    def __put(self, event):
        if self.__events.qsize() == self.__events.maxsize:
            self.__events.get_nowait()
        self.__events.put_nowait(event)

    @staticmethod
    def __get_seconds_gap_from_last_received_event(last_element, marshaled_event):
        last_dt = last_element[PerSecondLatestEventTracker.DATETIME_OBJ]
        curr_dt = marshaled_event[PerSecondLatestEventTracker.DATETIME_OBJ]
        td = curr_dt - last_dt
        seconds_gap = int(td.total_seconds())
        return seconds_gap

    def __overwrite_event(self, target, source):
        # print(self.__keys_to_track)
        for key in self.__keys_to_track:
            # print(key)
            target[key] = source.get(key)

    @staticmethod
    def __round_seconds(date_time_object):
        new_date_time = date_time_object

        if new_date_time.microsecond >= 500000:
            new_date_time = new_date_time + timedelta(seconds=1)

        return new_date_time.replace(microsecond=0)

    def __get_marshaled_event(self, json_event):
        marshaled_event = {}
        self.__overwrite_event(marshaled_event, json_event)

        current_event_time = json_event[self.__string_date_key]
        dt = datetime.strptime(current_event_time, '%Y-%m-%d %H:%M:%S')
        marshaled_event[PerSecondLatestEventTracker.DATETIME_OBJ] = self.__round_seconds(dt)

        return marshaled_event


class TransactionType(Enum):
    SHORT = 1
    LONG = 2


class MainClass:
    def __init__(self, file_name, median, price_percentage_diff, result_time: datetime, string_date_key):
        self._file_name = file_name
        self._median = median
        self._price_percentage_diff = price_percentage_diff
        self._result_time = result_time + timedelta(seconds=15)
        self._string_date_key = string_date_key
        self._base_filter_volume_threshold = self.get_vol_threshold(median, 6 * 60 * 60)
        self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12
        self._score_sum_threshold = 4
        self._market_open_time = self._result_time.replace(hour=9, minute=15, second=0)
        self._entry_price = None
        self._exit_price = None
        self._transaction_type = None

        # print("self._base_filter_volume_threshold: {}".format(self._base_filter_volume_threshold))

    def run(self):
        MarketChangeDetector(MarketEventEmitter(file_name=self._file_name), self._string_date_key, 15, self.base_filter,
                             [self.volume_score_function, self.long_price_quantity_score, self.result_score],
                             [self.volume_score_function, self.short_price_quantity_score, self.result_score],
                             self.score_filter, self.entry_function, self.exit_function).run()
        return {
            'entry': self._entry_price,
            'exit': self._exit_price,
            'type': str(self._transaction_type)
        }

    def base_filter(self, q: list) -> bool:
        start_end_vol_diff = self.start_end_diff(q, VOLUME)
        oldest_elem = q[0]
        oldest_elem_time = oldest_elem[PerSecondLatestEventTracker.DATETIME_OBJ]
        seconds_till_now = (oldest_elem_time - self._market_open_time).total_seconds()

        if seconds_till_now > 2 * 60 * 60:
            moving_vol_threshold = self.get_vol_threshold(oldest_elem[VOLUME], seconds_till_now)
            if moving_vol_threshold > self._base_filter_volume_threshold:
                self._base_filter_volume_threshold = moving_vol_threshold
                self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12

        return start_end_vol_diff > self._base_filter_volume_threshold

    @staticmethod
    def get_vol_threshold(vol_till_now, seconds_till_now):
        total_15_sec_slots = seconds_till_now / 15
        per_15_sec_slot_vol = vol_till_now / total_15_sec_slots
        return per_15_sec_slot_vol * 20

    def volume_score_function(self, q: list) -> int:
        score = 0
        for i in range(1, 5):
            score = score + 1 if q[-i][VOLUME] - q[-i - 1][VOLUME] > self._vol_diff_threshold_at_second_level \
                else score
        return score

    def result_score(self, q: list) -> int:
        q_time = q[-1][PerSecondLatestEventTracker.DATETIME_OBJ]
        td = q_time - self._result_time
        return (0 <= td.total_seconds() < 10 * 60) * 2

    def long_price_quantity_score(self, q: list) -> int:
        price_diff = self.start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = self.start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = self.start_end_diff(q, SELL_QUANTITY)

        score = 0
        if price_diff > 0 and self.price_diff_threshold_breached(price_diff, q):
            score = score + 1 if buy_quantity_diff > 0 else score
            score = score + 1 if sell_quantity_diff < 0 else score
        return score

    def short_price_quantity_score(self, q: list) -> int:
        price_diff = self.start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = self.start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = self.start_end_diff(q, SELL_QUANTITY)

        score = 0
        if price_diff < 0 and self.price_diff_threshold_breached(price_diff, q):
            score = score + 1 if buy_quantity_diff < 0 else score
            score = score + 1 if sell_quantity_diff > 0 else score
        return score

    def price_diff_threshold_breached(self, price_diff, q):
        return abs(price_diff) > (self._price_percentage_diff * q[0][LAST_PRICE]) / 100

    @staticmethod
    def start_end_diff(q, key):
        return q[-1][key] - q[0][key]

    def score_filter(self, score_list: list) -> bool:
        return all([score > 0 for score in score_list[:-1]]) * sum(score_list) > self._score_sum_threshold

    def entry_function(self, q: list, transaction_type: TransactionType):
        msg = "buy" if transaction_type == TransactionType.LONG else "sell"
        self._entry_price = q[-1][LAST_PRICE]
        self._transaction_type = transaction_type
        # print("{} stocks at : ".format(msg) + str(q[-1]))

    def exit_function(self, price) -> bool:
        diff = abs(self._entry_price - price)
        if diff > self._entry_price * 0.01:
            self._exit_price = price
            msg = "sell" if self._transaction_type == TransactionType.LONG else "buy"
            # print("{} stocks at : ".format(msg) + str(price))
            return True
        return False


if __name__ == '__main__':

    stat_file = "../market_analysis_data/stock_stats/combined_stats.json"
    with open(stat_file) as handle:
        stats = json.load(handle)

    summary_arr = csv_file_with_headers_to_json_arr("../market_analysis_data/summary.csv")
    names = list(map(lambda j_elem: j_elem['file_name'], summary_arr))


    def get_median(symbol):
        return list(filter(lambda j_elem: j_elem['stock_identifier'] == symbol, stats))[0]['volume_median']


    def get_result_time(symbol):
        time_elem = list(filter(lambda j_elem: j_elem['file_name'] == symbol, summary_arr))
        if len(time_elem) != 1:
            raise Exception("Bruh.. the result time is fucked up man.. just look at it: " + str(time_elem))
        return datetime.strptime("{} {}".format(time_elem[0]['date'], time_elem[0]['time_value']), '%Y-%m-%d %H:%M:%S')


    results = []


    def func(file_name, string_date_key):
        result = MainClass(file_name + ".csv", get_median(file_name), 0.2, get_result_time(file_name), string_date_key) \
            .run()
        result['file_name'] = file_name
        results.append(result)


    for name in names:
        try:
            try:
                func(name, "timestamp")
            except:
                func(name, "0.timestamp")
        except Exception as e:
            print("failed to process name: " + name + str(e))

    json_file_path = "../market_analysis_data/simulation_result.json"
    csv_file_path = "../market_analysis_data/simulation_result.csv"
    with open(json_file_path, 'w') as handle:
        json.dump(results, handle, indent=2)

    json_file_to_csv(json_file_path, csv_file_path)
