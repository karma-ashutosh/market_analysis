import datetime
import re
import requests
from bs4 import BeautifulSoup
import json


def parse_url(text: str) -> str:
    re.search("(?P<url>https?://[^\s]+)", text).group("url")


def get_bse_content_from_url(url) -> dict:
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    f = filter(lambda url: url.endswith(".pdf"),
               map(lambda x: x.get('href', ''), soup.findAll('a', attrs={'class': 'tablebluelink'})))
    pdf_links = list(f)
    company_details_soup = soup.find('td', attrs={'id': 'ctl00_ContentPlaceHolder1_tdCompNm'})

    security_code = company_details_soup.find('span', attrs={'class': 'spn02'}).text

    company_name_url_soup = company_details_soup.find('a', attrs={'class': 'tablebluelink'})
    company_name = company_name_url_soup.text
    company_page_url = company_name_url_soup.get('href', 'not found')
    company_details = {
        'pdf_links': pdf_links,
        'security_code': security_code,
        'company_name': company_name,
        'company_page_url': company_page_url,
        'original_url': url,
        'redirect_history': list(map(lambda x: x.url, r.history))
    }
    return company_details


def download_file(download_url, write_path):
    response = requests.get(download_url)
    file = open(write_path, 'wb')
    file.write(response.content)
    file.close()


def process_tweet(tweet: str):
    bse_url = parse_url(tweet)
    tweet_details = get_bse_content_from_url(bse_url)

    def get(key):
        return tweet_details.get(key)

    now = datetime.datetime.now()
    date_str = "{}-{}-{}".format(now.year, now.month, now.day)
    counter = 0
    file_paths = []
    for link in tweet_details.get('pdf_links'):
        file_path = "/tmp/{}_{}_{}_()".format(get('security_code'), get('company_name'), date_str, counter)
        file_paths.append(file_path)
        download_file(link, file_path)
        counter = counter + 1

