from datetime import datetime
from queue import Queue

from datetime import timedelta

from general_util import csv_file_with_headers_to_json_arr

BUY_QUANTITY = 'buy_quantity'
SELL_QUANTITY = "sell_quantity"
TIMESTAMP= ['',"0.timestamp", 'volume', 'buy_quantity','sell_quantity','last_trade_time','last_price']


class MarketEventEmitter:
    def __init__(self, file_name='spicejet.csv'):
        base_path = './../'

        j_arr = csv_file_with_headers_to_json_arr(base_path + file_name)
        self.__event_list = list(
            map(lambda j_elem: self.__remove_keys(j_elem, ['depth', 'Unnamed', 'instrument_token', 'mode', 'ohlc', 'oi_day',
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
    def __init__(self, event_emitter: MarketEventEmitter):
        self._event_emitter = event_emitter
        keys_to_track = [TIMESTAMP]
        self._event_window_15_sec = PerSecondLatestEventTracker(window_length_in_seconds=15, keys_to_track=keys_to_track, string_date_key= '0.timestamp')
        self._event_window_1_min = PerSecondLatestEventTracker(window_length_in_seconds=60, keys_to_track=keys_to_track,string_date_key= '0.timestamp')
        self._event_window_10_min = PerSecondLatestEventTracker(window_length_in_seconds=600, keys_to_track=keys_to_track,string_date_key= '0.timestamp')
        self._event_window_60_min = PerSecondLatestEventTracker(window_length_in_seconds=3600, keys_to_track=keys_to_track,string_date_key= '0.timestamp')

    def run(self):
        while True:
            try:
                market_event = self._event_emitter.emit()
                # print(market_event,'aaa')
                self._event_window_15_sec.move(market_event)

                self._event_window_10_min.move(market_event)

                a= self._event_window_15_sec.get_current_queue_snapshot()

                print(int(a[-1]['volume'])-int(a[0]['volume']),'\t', a[-1]['datetime']-a[0]['datetime'],a[-1]['datetime'],a[0]['datetime'],'\t',a[0]['last_price'],a[-1]['last_price'],'\t', a[-1]['volume'],'\t', a[-1]['buy_quantity'],a[-1]['sell_quantity'])

                avg_15_sec = self._event_window_15_sec.get_avg(key=TIMESTAMP)
                avg_10_min = self._event_window_10_min.get_avg(key=TIMESTAMP)

                if avg_15_sec < avg_10_min:
                    print("There is a rise in the share at market_event: {}".format(market_event))
                    print("15 sec avg: {} and 10 min avg: {}".format(avg_15_sec, avg_10_min))
            except StopIteration as e:
                print(e)
                print("Out of events to emit. Market closes. Go smoke all that money")
                break


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
            return [elem/self.__window_length for elem in self._sum]


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
        for key in self.__keys_to_track[0]:
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

    def get_avg(self, key=None):
        if key:
            key_pos = self.__keys_to_track.index(key)
            return self._sum[key_pos] / self.__window_length
        else:
            return [elem/self.__window_length for elem in self._sum]


def main():
    # print('hees')
    # keys = ['', 'last_price', 'last_quantity', 'volume', 'buy_quantity', 'sell_quantity', 'last_trade_time',
    #         '0.timestamp','average_price', 'change', 'oi', 'Delta']
    # keys= ['volume']
    event_emitter = MarketEventEmitter(file_name='spicejet.csv')
    # print(event_emitter.emit())

    # tracker = PerSecondLatestEventTracker(5, keys, 'timestamp')

    MarketChangeDetector(event_emitter).run()
    return event_emitter, tracker



if __name__ == '__main__':
    print(len(main()[1].get_current_queue_snapshot()))