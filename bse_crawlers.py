import json
from time import sleep

import requests
import urllib3
import yaml
from bs4 import BeautifulSoup
from dateparser import parse
from general_util import csv_file_with_headers_to_dict_list
# todo https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
from postgres_io import PostgresIO

http = urllib3.PoolManager()


class UpComingResultCrawler:

    def __init__(self):
        pass

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
        instrument_mappings = csv_file_with_headers_to_dict_list("text_files/instruments.csv")
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


if __name__ == '__main__':
    symbols_to_process = [line.strip() for line in
                          open('text_files/temporary_stock_symbols_to_process.txt').readlines()]

    parser = HistoricalStockPriceParser()
    symbol_to_bse_script_id_mapping = parser.get_trading_sym_to_exchange_script_id_mapping()
    script_ids_to_process = list(map(lambda sym: symbol_to_bse_script_id_mapping.get(sym), symbols_to_process))
    failed_indexes = list(filter(lambda index: script_ids_to_process[index] is None, range(len(script_ids_to_process))))
    for index in range(len(script_ids_to_process)):
        if index not in failed_indexes:
            result_arr = parser.parse(script_ids_to_process[index])
            with open("crawed_data_output/{}.json".format(symbols_to_process[index]), 'w') as handle:
                json.dump(result_arr, handle, indent=2)

