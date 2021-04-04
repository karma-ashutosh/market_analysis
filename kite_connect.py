import yaml
from kiteconnect import KiteConnect
import requests
import json

from kite_util import KiteUtil
from postgres_io import PostgresIO

with open('./config.yml') as handle:
    config = yaml.load(handle)
postgres = PostgresIO(config['postgres-config'])
# postgres.connect()

# bse = BseUtil(config, postgres)
k_util = KiteUtil(postgres, config)

session_info = k_util.get_current_session_info()['result'][0]
api_key, access_token = session_info['api_key'], session_info['access_token']

kite = KiteConnect(api_key=api_key)
kite.login_url()

headers = {'X-Kite-Version': '3', 'Authorization': 'token {}:{}'.format(api_key, access_token)}

instruments_path = "/home/karma/Development/market_analysis/text_files/instruments.csv"


def update_instruments():
    ############## save instruments data ######################
    instrument_list_url = "https://api.kite.trade/instruments"
    res_instrument = requests.get(instrument_list_url, headers=headers)
    f = open(instruments_path, 'w')
    f.write(res_instrument.text)
    f.flush()
    f.close()

############## read instruments data ######################
def some_function_for_filtering():
    instrument_lines = [line.strip().split(",") for line in open(instruments_path).readlines()]
    target_symbols = ["FEL", "ALBERTDA", "INDLMETER", "DHUNINV", "GEOJITFSL", "APMIN", "PARABDRUGS", "PONNIERODE",
                      "IVRCLINFRA", "CROMPTON",
                      "NEULANDLAB", "NAVKARCORP", "SALASAR", "THEMISMED", "ONELIFECAP", "ACC", "NESTLEIND", "GODREJIND",
                      "VIKRAMTH", "BASANTGL", "MCLEODRUSS*",
                      "DANLAW", "EICHERMOT", "INDTERRAIN", "FERVENTSYN", "RAJABAH", "PRICOLLTD", "KINETICENG", "PML",
                      "WANBURY", "UNITDSPR", "KSL", "RODIUM",
                      "RACLGEAR"]

    target_symbol_set = set(target_symbols)

    target_instruments = list(filter(lambda x: x[-1] == 'BSE' and x[2] in target_symbol_set, instrument_lines))


url = "https://api.kite.trade/instruments/historical/5633/5minute?from=2015-12-28+09:30:00&to=2016-01-01+10:30:00"
share_history_url_format = "https://api.kite.trade/instruments/historical/{}/{}?from={}&to={}"


def date_range(start, end, intv):
    from datetime import datetime
    start = datetime.strptime(start,"%Y%m%d")
    end = datetime.strptime(end,"%Y%m%d")
    diff = (end - start) / intv
    for i in range(intv):
        yield (start + diff * i).strftime("%Y-%m-%d")
    yield end.strftime("%Y-%m-%d")


def get_date_ranges() -> list:
    end_date = "20200401"
    start_date = "20210331"
    number_of_days = 365  # got online
    max_interval_length_days_for_zerodha_api = 80
    number_of_intervals = int(number_of_days / max_interval_length_days_for_zerodha_api) + 1
    intervals = date_range(start_date, end_date, number_of_intervals)
    intervals = list(reversed(list(map(lambda x: x + "+00:00:00", intervals))))
    return list(zip(intervals, intervals[1:]))


def get_share_price_info(meta_line):
    instrument_code = meta_line[0]
    company_name = meta_line[2]
    # start_date = meta_line[8]
    # end_date = meta_line[9]

    # overriding start and end date to retrieve last 5 years of data
    # start_date = "2020-04-01+00:00:00"
    # end_date = "2021-03-31+00:00:00"

    date_ranges = get_date_ranges()
    result = []
    for i in range(len(date_ranges)):
        (start_date, end_date) = date_ranges[i]
        share_history_url = share_history_url_format.format(instrument_code, "day", start_date, end_date)
        res = requests.get(share_history_url, headers=headers)

        try:
            j_arr = json.loads(res.text)
            data = j_arr['data']['candles']
            result.extend(data)
            # f = open("/data/kite_websocket_data/historical/{}_{}_{}_part_{}.tsv"
            #          .format(company_name, start_date, end_date, i), 'w')
            # for entry in data:
            #     f.write("\t".join(list(map(lambda x: str(x), entry))) + "\n")
            # f.flush()
            # f.close()

        except:
            print("Error while processing for interval {} - {}\n".format(start_date, end_date) + res.text + "\n" +
                  str(res.request) + "\n" + res.url)
    with open('/data/kite_websocket_data/historical/{}_{}.json'.format(company_name, instrument_code), 'w') as handle:
        json.dump(result, handle, indent=1, default=str)





if __name__ == '__main__':
    ################# share price data #######################

    meta_lines = [line.strip().split("\t") for line in
                  open("text_files/nifty_instruments.tsv").readlines()[1:]]

    for meta_line in meta_lines:
        try:
            get_share_price_info(meta_line)
        except Exception as e:
            print("could not process meta_line: {}".format(meta_line))
            raise e
