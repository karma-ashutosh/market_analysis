from datetime import datetime, timedelta
import json

import requests
import yaml

from connection_factory import ConnectionFactory
from constants import TextFileConstants, URLS

with open('./config.yml') as handle:
    config = yaml.load(handle)

factory = ConnectionFactory(config)

# factory.init_bse_util()
# bse = factory.bse_util

factory.init_kite()
headers = factory.kite_headers


def update_instruments():
    ############## save instruments data ######################
    instrument_list_url = "https://api.kite.trade/instruments"
    res_instrument = requests.get(instrument_list_url, headers=headers)
    f = open(TextFileConstants.KITE_INSTRUMENTS, 'w')
    f.write(res_instrument.text)
    f.flush()
    f.close()


def get_date_ranges(start_date, end_date, end_date_minus_start_date) -> list:
    def date_range(intv):
        start = datetime.strptime(_start_date, "%Y%m%d")
        end = datetime.strptime(_end_date, "%Y%m%d")
        diff = (end - start) / intv
        for i in range(intv):
            yield (start + diff * i).strftime("%Y-%m-%d")
        yield end.strftime("%Y-%m-%d")

    _start_date, _end_date = end_date, start_date
    max_interval_length_days_for_zerodha_api = 80
    number_of_intervals = int(end_date_minus_start_date / max_interval_length_days_for_zerodha_api) + 1
    intervals = date_range(number_of_intervals)
    intervals = list(reversed(list(map(lambda x: x + "+00:00:00", intervals))))
    return list(zip(intervals, intervals[1:]))


def get_historical_share_ohlc(instrument_code, date_ranges: list) -> list:
    result = []
    for i in range(len(date_ranges)):
        (start_date, end_date) = date_ranges[i]
        share_history_url = URLS.KITE_SHARE_HISTORY_URL_FORMAT.format(instrument_code, "day", start_date, end_date)
        res = requests.get(share_history_url, headers=headers)

        try:
            j_arr = json.loads(res.text)
            data = j_arr['data']['candles']
            result.extend(data)
        except:
            print("Error while processing for interval {} - {}\n".format(start_date, end_date) + res.text + "\n" +
                  str(res.request) + "\n" + res.url)
    return result


class Nify50LastNDaysDownloader:
    def __init__(self, number_of_days=60):
        self.number_of_days = number_of_days

    def download(self):
        # we should ideally read the existing file, fetch the delta days worth of data and append that to existing
        # file. This way if we can get just daily ohlc, we can maintain our file and not pay kite for 2k extra per
        # month. For now skipping this and simply using historical API on every run for preparing last 2 month of
        # data
        for instrument_file_name in TextFileConstants.NIFTY_50_DATA_FILE_NAMES:
            today = datetime.now()
            two_month_back = today - timedelta(days=self.number_of_days)
            start_date = two_month_back.strftime("%Y%m%d")
            end_date = today.strftime("%Y%m%d")
            date_ranges = get_date_ranges(start_date, end_date, self.number_of_days)
            share_info = get_historical_share_ohlc(instrument_file_name.replace(".json", "").split("_")[1], date_ranges)
            with open(TextFileConstants.KITE_CURRENT_DATA + instrument_file_name, 'w') as handle:
                json.dump(share_info, handle, indent=1, default=str)


if __name__ == '__main__':
    meta_lines = [line.strip().split("\t") for line in
                  open(TextFileConstants.NIFTY_50_INSTRUMENTS).readlines()[1:]]

    start_date = "20190401"
    end_date = "20200331"
    delta_days = 365
    date_ranges = get_date_ranges(start_date, end_date, delta_days)
    for meta_line in meta_lines:
        try:
            instrument_code = meta_line[0]
            company_name = meta_line[2]

            share_info = get_historical_share_ohlc(instrument_code, date_ranges)
            with open(TextFileConstants.KITE_DATA_BASE_DIR + '/historical/2019_20/{}_{}.json'
                    .format(company_name, instrument_code), 'w') as handle:
                json.dump(share_info, handle, indent=1, default=str)
        except Exception as e:
            print("could not process meta_line: {}".format(meta_line))
            raise e
