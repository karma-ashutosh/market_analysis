from datetime import datetime
from queue import Queue

from datetime import timedelta

from general_util import csv_file_with_headers_to_json_arr

EMPTY_KEY = ''

LAST_PRICE = 'last_price'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

TIMESTAMP = "0.timestamp"

VOLUME = 'volume'


class MarketEventEmitter:
    def __init__(self, file_name='spicejet.csv'):
        base_path = '/Users/ashutosh.v/Development/market_analysis_data/'

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
        return next(self.__event_iter)


class MarketChangeDetector:
    def __init__(self, event_emitter: MarketEventEmitter, window_len, base_filter_func, score_func_list: list,
                 score_filter_func, post_processor_func):
        self._event_emitter = event_emitter
        self._base_filter_func = base_filter_func
        self._score_func_list = score_func_list
        self._score_filter_func = score_filter_func
        self._post_processor_func = post_processor_func

        self._string_date_key = '0.timestamp'

        keys_to_track = [EMPTY_KEY, TIMESTAMP, VOLUME, BUY_QUANTITY, SELL_QUANTITY, LAST_TRADE_TIME, LAST_PRICE]
        self._event_window_15_sec = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                                keys_to_track=keys_to_track,
                                                                string_date_key=self._string_date_key)

        self._filter_pass_key_name = 'bool'
        self._filter_pass_queue = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                              keys_to_track=[self._filter_pass_key_name],
                                                              string_date_key=self._string_date_key)

    def run(self):
        def set_flag_with_base_filter_func():
            if self._base_filter_func(current_event_list_view):
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            else:
                self._filter_pass_queue.move(self.get_filter_event(market_event, False))

        while True:
            try:
                market_event = self._event_emitter.emit()
                self._event_window_15_sec.move(market_event)
                current_event_list_view = self._event_window_15_sec.get_current_queue_snapshot()

                if any([e[self._filter_pass_key_name] for e in self._filter_pass_queue.get_current_queue_snapshot()]):
                    all_scores = list(map(lambda func: func(current_event_list_view), self._score_func_list))
                    if self._score_filter_func(all_scores):
                        self._post_processor_func(current_event_list_view)
                        self._filter_pass_queue.move(self.get_filter_event(market_event, True))
                    else:
                        set_flag_with_base_filter_func()
                else:
                    set_flag_with_base_filter_func()

            except StopIteration as e:
                print(e)
                print("Out of events to emit. Market closes. Go smoke all that money")
                break

    def get_filter_event(self, market_event, state: bool):
        filter_event = {
            self._string_date_key: market_event.get(self._string_date_key),
            self._filter_pass_key_name: state
        }
        return filter_event


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


def main():
    base_filter_volume_threshold = 12000
    vol_diff_threshold_at_second_level = 1000
    score_sum_threshold = 4

    event_emitter = MarketEventEmitter(file_name='spicejet.csv')

    def base_filter(q: list) -> bool:
        return start_end_diff(q, VOLUME) > base_filter_volume_threshold

    def score_func_1(q: list) -> int:
        score = 0
        for i in range(1, 5):
            score = score + 1 if float(q[-i][VOLUME]) - float(q[-i - 1][VOLUME]) > vol_diff_threshold_at_second_level \
                else score
        return score

    def score_func_2(q: list) -> int:
        price_diff = start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = start_end_diff(q, SELL_QUANTITY)
        return price_diff > 0 * (buy_quantity_diff > 0 + sell_quantity_diff < 0)

    def result_score(q: list) -> int:
        return 1

    def start_end_diff(q, key):
        return float(q[-1][key]) - float(q[0][key])

    def score_filter(score_list: list) -> bool:
        return all([score > 0 for score in score_list]) * sum(score_list) > score_sum_threshold

    def post_processor(q: list):
        print("bought dher sara stocks at : "+str(q[-1]))

    MarketChangeDetector(event_emitter, 15, base_filter, [score_func_1, score_func_2, result_score], score_filter,
                         post_processor).run()
    return event_emitter


if __name__ == '__main__':
    main()
