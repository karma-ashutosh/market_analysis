from time import sleep

import requests
import urllib3
import yaml
from bs4 import BeautifulSoup
from dateparser import parse

# todo https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
from postgres_io import PostgresIO

http = urllib3.PoolManager()


def get_announcements_list() -> list:
    announcement_page = "https://www.bseindia.com/corporates/Forth_Results.aspx?expandable=0"
    r = requests.get(announcement_page)
    soup = BeautifulSoup(r.text, "html.parser")
    html_table = soup.find('table', attrs={'id': 'ctl00_ContentPlaceHolder1_gvData'})
    table_rows = list(map(lambda x: x.findAll("td"), html_table.findAll("tr")))
    visible_data = [[e.text for e in elements] for elements in table_rows][1:]  # ignoring the header column
    return visible_data


def days_to_seconds(number_of_days):
    return number_of_days * 24 * 60 * 60


def parse_and_insert_data(postgres: PostgresIO):
    parsed_data = get_announcements_list()
    j_arr = []
    for entry in filter(lambda x: len(x) is 3, parsed_data):
        result_timestamp_seconds = parse(entry[2]).timestamp()
        j_arr.append(
            {
                'exchange': 'BSE',
                'security_code': entry[0],
                'symbol': entry[1],
                'result_date': entry[2],
                'hourly_crawling_start_timestamp': str(result_timestamp_seconds - days_to_seconds(7)),
                'hourly_crawling_stop_timestamp': str(result_timestamp_seconds),
                'minute_crawling_start_timestamp': str(result_timestamp_seconds),
                'minute_crawling_stop_timestamp': str(result_timestamp_seconds + days_to_seconds(2)),
                'crawling_done': 'false',

            }
        )
    postgres.insert_or_skip_on_conflict(j_arr, 'share_market_data.upcoming_results', ['symbol', 'result_date'])


if __name__ == '__main__':
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()
    while True:
        parse_and_insert_data(postgres)
        sleep(24*60*60)
