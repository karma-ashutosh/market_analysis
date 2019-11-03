import json
from datetime import datetime
from datetime import timedelta
# todo https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
from statistics import mean, median
from time import sleep

import requests
import yaml
from bs4 import BeautifulSoup
from dateparser import parse

from general_util import csv_file_with_headers_to_json_arr
from postgres_io import PostgresIO
from result_date_object import ResultDate


def get_bse_url_compatible_date(date_time_obj: datetime):
    return "{}{}{}".format(str(date_time_obj.year).zfill(4), str(date_time_obj.month).zfill(2),
                           str(date_time_obj.day).zfill(2))


def get_system_readable_date(date_time_obj):
    return '{}-{}-{}'.format(str(date_time_obj.year).zfill(4), str(date_time_obj.month).zfill(2),
                             str(date_time_obj.day).zfill(2))


def system_readable_today():
    today = datetime.today()
    return get_system_readable_date(today)


class BseAnnouncementCrawler:
    def __init__(self, postgres: PostgresIO, config: dict):
        self._postgres = postgres
        self._announcement_table = config['bse_config']['announcements_table']
        self._system_readable_date_key = 'system_readable_date'
        self._news_id_key = 'news_id'
        self._keys_to_copy_in_table = [('NEWSID', self._news_id_key),
                                       ('SCRIP_CD', 'security_code'),
                                       ('NEWS_DT', 'news_datetime'),
                                       ('ATTACHMENTNAME', 'attachment_name')]

    def refresh(self):
        system_readable_date = system_readable_today()
        all_announcements = self._get_todays_board_meeting_updates()
        all_announcements.extend(self._get_todays_result_announcements_updates())
        payload_arr = list(map(lambda j: self._get_payload_from_bse_data(j, system_readable_date), all_announcements))
        already_captured_news_ids = self.__get_already_stored_news_ids(system_readable_date)
        new_announcements = list(filter(lambda j: j[self._news_id_key] not in already_captured_news_ids, payload_arr))
        self._save_to_database(new_announcements)

    def get_company_announcement_map_for_today(self) -> dict:
        query = "SELECT * FROM {} WHERE {}='{}'".format(
            self._announcement_table,
            self._system_readable_date_key,
            system_readable_today()
        )
        todays_captured_announcements = self._postgres.execute([query], fetch_result=True)['result']
        result = {}
        for j_elem in todays_captured_announcements:
            key = j_elem['security_code']
            if key not in result.keys() or result[key]['news_datetime'] < j_elem['news_datetime']:
                result[key] = j_elem
        return result

    def get_latest_result_time_for_security_code(self, security_code):
        query = "SELECT * FROM {} WHERE {}='{}' AND {}='{}'".format(
            self._announcement_table,
            self._system_readable_date_key, system_readable_today(),
            'security_code', security_code
        )
        todays_captured_announcements = self._postgres.execute([query], fetch_result=True)['result']
        result = {}
        for j_elem in todays_captured_announcements:
            key = j_elem['security_code']
            if key not in result.keys() or result[key]['news_datetime'] < j_elem['news_datetime']:
                result[key] = j_elem
        return result


    def _save_to_database(self, announcements):
        self._postgres.insert_jarr(announcements, self._announcement_table)

    def _get_payload_from_bse_data(self, j_elem, system_readable_date):
        output = {}
        for key in self._keys_to_copy_in_table:
            output[key[1]] = j_elem[key[0]]
        output[self._system_readable_date_key] = system_readable_date
        return output

    def __get_already_stored_news_ids(self, system_readable_date) -> set:
        query = "SELECT {} FROM {} WHERE {}='{}'".format(
            self._news_id_key,
            self._announcement_table,
            self._system_readable_date_key,
            system_readable_date
        )
        already_captured_news_ids = set(map(
            lambda j: j[self._news_id_key], self._postgres.execute([query], fetch_result=True)['result']
        ))
        return already_captured_news_ids

    @staticmethod
    def _get_todays_board_meeting_updates() -> list:
        today = datetime.today()
        bse_compatible_date = get_bse_url_compatible_date(today)
        url = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat={}&strPrevDate={}&strScrip=&strSearch=P" \
              "&strToDate={}&strType=C".format("Board Meeting", bse_compatible_date, bse_compatible_date)
        return requests.get(url).json()['Table']

    @staticmethod
    def _get_todays_result_announcements_updates() -> list:
        today = datetime.today()
        bse_compatible_date = get_bse_url_compatible_date(today)
        url = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat={}&strPrevDate={}&strScrip=&strSearch=P" \
              "&strToDate={}&strType=C".format("Result", bse_compatible_date, bse_compatible_date)
        return requests.get(url).json()['Table']


class BseUtil:
    def __init__(self, config: dict, postgres: PostgresIO):
        self.__config = config
        self.__postgres = postgres
        self.__upcoming_results_date_table = config['bse_send_result_notification']['upcoming_result_table']
        self.__stock_date_wise_monitoring_time_window = self.__prepare_significant_stock_time_ranges()
        self.__stats_table = config['bse_config']['market_stat_table']

    def get_stat(self, security_code):
        return self.__postgres.execute(["SELECT * FROM {} WHERE security_code = '{}'"
                                       .format(self.__stats_table, security_code)],
                                       fetch_result=True)['result']

    def get_all_stats(self):
        return self.__postgres.execute(["SELECT * FROM {}".format(self.__stats_table)], fetch_result=True)['result']

    def __prepare_significant_stock_time_ranges(self) -> dict:
        historical_result_file = self.__config['bse_send_result_notification']['historical_result_time_info_file']
        j_arr = csv_file_with_headers_to_json_arr(historical_result_file)
        result = {}
        for j_elem in j_arr:
            dt = datetime.strptime(j_elem.get('date_time'), '%Y-%m-%d %H:%M:%S')
            j_elem['end_time'] = dt + timedelta(minutes=30)
            j_elem['start_time'] = dt - timedelta(minutes=30)
            result["{}-{}".format(j_elem.get("stock_symbol"), dt.date())] = j_elem
        return result

    def get_result_announcement_meta_for_today(self):
        today = datetime.now()
        today_date = get_system_readable_date(today)
        upcoming_results_query = "SELECT * FROM {} WHERE system_readable_date='{}'" \
            .format(self.__upcoming_results_date_table, today_date)
        result_list = self.__postgres.execute([upcoming_results_query], fetch_result=True).get('result', [])
        return result_list

    def get_result_announcement_meta_for_yesterday(self):
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_date = get_system_readable_date(yesterday)
        upcoming_results_query = "SELECT * FROM {} WHERE system_readable_date='{}'" \
            .format(self.__upcoming_results_date_table, yesterday_date)
        result_list = self.__postgres.execute([upcoming_results_query], fetch_result=True).get('result', [])
        return result_list

    def should_process_historical_event(self, stock_code: str, event_time: datetime) -> bool:
        result = False
        key = "{}-{}".format(stock_code, event_time.date())
        if key in self.__stock_date_wise_monitoring_time_window.keys():
            j_elem = self.__stock_date_wise_monitoring_time_window.get(key)
            if j_elem['start_time'] <= event_time <= j_elem['end_time']:
                result = True
        return result


def get_announcement_for_stock_for_date_range(stock_code, from_date, to_date) -> list:
    announcement_url_format = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate={" \
                              "}&strScrip={}&strSearch=P&strToDate={}&strType=C "
    url = announcement_url_format.format(from_date, stock_code, to_date)
    r = requests.get(url)
    json_res = json.loads(r.text)
    result = []
    if json_res.get('Table'):
        result = json_res.get('Table')
    return result


class BseResultUpdateUtil:
    def __init__(self, postgres: PostgresIO, config: dict):
        self.bse_config = config['bse_config']
        self.postgres = postgres

    def run(self):
        j_arr = self.extract_jarr_from_file('text_files/result_dates.txt')

        filtered_stock_list = set(map(lambda j_elem: j_elem['security_code'], self.postgres.execute(
            ["SELECT security_code from {}".format(self.bse_config['filtered_stock_list_table'])],
            fetch_result=True)['result']))

        target_j_arr = list(filter(lambda j_elem: j_elem['security_code'] in filtered_stock_list, j_arr))
        print("{} stocks qualified for result tracking out of {} stocks provided".format(len(target_j_arr), len(j_arr)))
        self.postgres.insert_or_skip_on_conflict(target_j_arr, self.bse_config['upcoming_result_table'],
                                                 ['security_code', 'result_date'])

    @staticmethod
    def get_human_readable_date(date: str) -> str:
        return datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')

    @staticmethod
    def format_date_for_code(nse_date_string: str) -> str:
        return nse_date_string.replace('Apr', 'April').replace('Jun', 'June').replace('Jul', 'July') \
            .replace('Aug', 'August').replace('Oct', 'October')

    def get_insert_json(self, line: str) -> dict:
        print("parsing")
        arr = line.split("\t")
        security_code, security_name, result_date = arr[0], arr[1], self.format_date_for_code(arr[2])
        return {
            'security_code': security_code,
            'security_name': security_name,
            'result_date': result_date,
            'pre_delta_days': 1,
            'post_delta_days': 1,
            'system_readable_date': self.get_human_readable_date(result_date)
        }

    def extract_jarr_from_file(self, file_path: str):
        lines = map(lambda line: line.strip(), open(file_path).readlines())
        j_arr = list(map(self.get_insert_json, lines))
        return j_arr


class HistoricalBseAnnouncements:
    def __init__(self, postgres: PostgresIO, config: dict):
        self.postgres = postgres
        self.bse = BseUtil(config, postgres)
        self.upcoming_results_date_table = config['bse_send_result_notification']['upcoming_result_table']
        self.bse_notification_checkpointing_table = config['bse_send_result_notification']['checkpointing_table']

    def get_result_announcements_date_range(self, from_date, to_date):
        query = [
            "SELECT security_code, system_readable_date FROM {} WHERE system_readable_date >= '{}' and "
            "system_readable_date <= '{}'".format(self.upcoming_results_date_table, from_date, to_date)]
        rows = self.postgres.execute(query, fetch_result=True)['result']
        announcement_list = []
        for row in rows:
            result_date = ResultDate(row)
            announcement_date = result_date.system_readable_date.replace("-", "")
            announcement = get_announcement_for_stock_for_date_range(result_date.security_code, announcement_date,
                                                                     announcement_date)
            if announcement:
                announcement_list.append(announcement)
        return announcement_list

    def run(self):
        from_date = input("Enter the from date (in format '2019-08-04'):\t")
        to_date = input("Enter the to date (in format '2019-08-04'):\t")
        output_location = input("output location to save your data (example /tmp/result.json): ")

        announcement_list = self.get_result_announcements_date_range(from_date, to_date)
        output = []
        for announcement in announcement_list:
            output.extend(announcement)
        with open(output_location, 'w') as h:
            json.dump(output, h, indent=1)


@DeprecationWarning
class UpComingResultCrawler:
    def __init__(self):
        raise Exception("The class is buggy and not tested. Fix this")

    @staticmethod
    def get_announcements_list() -> list:
        announcement_page = "https://www.bseindia.com/corporates/Forth_Results.aspx?expandable=0"
        r = requests.get(announcement_page)
        soup = BeautifulSoup(r.text, "html.parser")
        html_table = soup.find('table', attrs={'id': 'ctl00_ContentPlaceHolder1_gvData'})
        table_rows = list(map(lambda x: x.findAll("td"), html_table.findAll("tr")))
        visible_data = [[e.text for e in elements] for elements in table_rows][1:]  # ignoring the header column
        return visible_data

    @staticmethod
    def days_to_seconds(number_of_days):
        return number_of_days * 24 * 60 * 60

    def parse_and_insert_data(self, postgres: PostgresIO):
        parsed_data = self.get_announcements_list()
        j_arr = []
        for entry in filter(lambda x: len(x) is 3, parsed_data):
            result_timestamp_seconds = parse(entry[2]).timestamp()
            j_arr.append(
                {
                    'exchange': 'BSE',
                    'security_code': entry[0],
                    'symbol': entry[1],
                    'result_date': entry[2],
                    'hourly_crawling_start_timestamp': str(result_timestamp_seconds - self.days_to_seconds(7)),
                    'hourly_crawling_stop_timestamp': str(result_timestamp_seconds),
                    'minute_crawling_start_timestamp': str(result_timestamp_seconds),
                    'minute_crawling_stop_timestamp': str(result_timestamp_seconds + self.days_to_seconds(2)),
                    'crawling_done': 'false',
                }
            )
        postgres.insert_or_skip_on_conflict(j_arr, 'share_market_data.upcoming_results', ['symbol', 'result_date'])

    def run(self):
        with open('./config.yml') as handle:
            config = yaml.load(handle)
        postgres = PostgresIO(config['postgres-config'])
        postgres.connect()
        while True:
            self.parse_and_insert_data(postgres)
            sleep(24 * 60 * 60)


class HistoricalStockPriceParser:
    def __init__(self):
        pass

    @staticmethod
    def extract_all_values_in_order(row) -> list:
        cells = row.findAll("td")
        return [cell.text for cell in cells]

    @staticmethod
    def get_trading_sym_to_exchange_script_id_mapping():
        instrument_mappings = csv_file_with_headers_to_json_arr("text_files/instruments.csv")
        symbol_to_bse_script_id_mapping = {}
        for j_elem in instrument_mappings:
            if j_elem['exchange'] == 'BSE':
                symbol_to_bse_script_id_mapping[j_elem['tradingsymbol']] = j_elem['exchange_token']
        return symbol_to_bse_script_id_mapping

    def parse(self, script_code):
        url = "https://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?expandable=6&scripcode={}" \
              "&flag=sp&Submit=G".format(script_code)
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.findAll("table")[-2]
        table_rows = table.findAll("tr")
        header_row = table_rows[0]
        column_names = self.extract_all_values_in_order(header_row)
        data_rows = [self.extract_all_values_in_order(row) for row in table_rows[2:]]
        result = [dict(zip(column_names, row_values)) for row_values in data_rows]
        return result

    def run(self):
        symbols_to_process = [line.strip() for line in
                              open('text_files/temporary_stock_symbols_to_process.txt').readlines()]

        symbol_to_bse_script_id_mapping = self.get_trading_sym_to_exchange_script_id_mapping()
        script_ids_to_process = list(map(lambda sym: symbol_to_bse_script_id_mapping.get(sym), symbols_to_process))
        failed_indexes = list(
            filter(lambda index: script_ids_to_process[index] is None, range(len(script_ids_to_process))))

        generated_file_names = []
        for index in range(len(script_ids_to_process)):
            if index not in failed_indexes:
                result_arr = self.parse(script_ids_to_process[index])
                f_name = symbols_to_process[index]
                generated_file_names.append(f_name)
                with open("crawled_data_output/{}.json".format(f_name), 'w') as handle:
                    json.dump(result_arr, handle, indent=2)

        stat_list = []
        for f_name in generated_file_names:
            try:
                path = "crawled_data_output/{}.json".format(f_name)
                with open(path) as handle:
                    j = json.load(handle)
                stats = {
                    'name': f_name
                }
                stats.update(_get_stats("trades", [float(elem["No. of Trades"].replace(",", "")) for elem in j]))
                stats.update(_get_stats("volume", [float(elem["No. of Shares"].replace(",", "")) for elem in j]))
                stats.update(_get_stats("close", [float(elem["Close"].replace(",", "")) for elem in j]))
                stat_list.append(stats)
            except:
                print("failed for f_name: {}".format(f_name))

        output_file = "crawled_data_output/crawled_data_stats.json"
        print("writing stats to file: {}".format(output_file))
        with open(output_file, 'w') as handle:
            json.dump(stat_list, handle, indent=2)


def _get_stats(stat_identifier_prefix: str, data_points: list):
    if data_points:
        return {
            stat_identifier_prefix + "_min": min(data_points),
            stat_identifier_prefix + "_max": max(data_points),
            stat_identifier_prefix + "_mean": mean(data_points),
            stat_identifier_prefix + "_median": median(data_points)
        }
    else:
        return {}


if __name__ == '__main__':
    choice = int(input("Select your choice. Type: "
                       "(i) 1 for update upcoming result dates\n"
                       "(ii) 2 for Getting bse annoucnements for a date range\n"
                       "(iii) 3 for getting historical day wise stock prices for instruments under column "
                       "exchange_token in file text_files/instruments.csv\n "
                       "(iv) 4 for updating new bse announcements in db\n"
                       "(v) 5 for getting company wise latest news\n"))

    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()

    if choice == 1:
        BseResultUpdateUtil(postgres, config).run()
    elif choice == 2:
        HistoricalBseAnnouncements(postgres, config).run()
    elif choice == 3:
        HistoricalStockPriceParser().run()
    elif choice == 4:
        BseAnnouncementCrawler(postgres, config).refresh()
    elif choice == 5:
        print(BseAnnouncementCrawler(postgres, config).get_company_announcement_map_for_today())
    else:
        print("Choice didn't match any of the valid options. Please try again")
