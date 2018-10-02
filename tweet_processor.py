import datetime
import logging
import re

import requests
import yaml
from bs4 import BeautifulSoup

from general_util import send_mail
from postgres_io import PostgresIO

with open('./config.yml') as handle:
    config = yaml.load(handle)
feed_table = config['postgres-config']['twitter.feed.table']
data_dir = config['twitter-config']['tweet_data_dir']
postgres = PostgresIO(config['postgres-config'])
postgres.connect()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

mail_username = config['email-config']['username']
mail_password = config['email-config']['password']


def parse_url(text: str) -> str:
    return re.search("(?P<url>https?://[^\s]+)", text).group("url")


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
        file_path = "{}/{}_{}_{}_{}.pdf".format(data_dir, get('security_code'), get('company_name'), date_str, counter)
        file_paths.append(file_path)
        download_file(link, file_path)
        counter = counter + 1
    send_mail(mail_username, mail_password, "Announcement for {}".format(get('company_name')),
              "tweet url: {}".format(tweet),
              ["tanmayiitj@gmail.com", "prateektagde@gmail.com", "karmav44990@gmail.com"], file_paths)


def process_new_tweets():
    query = "SELECT user_text, user_status_id from {} WHERE processed = false"
    results = postgres.execute([query.format(feed_table)], fetch_result=True)['result']
    for result in results:
        try:
            url = parse_url(result.get("user_text"))
            logging.info("processing user status id: {}".format(result.get("user_status_id")))
            process_tweet(url)
            postgres.execute(["UPDATE {} SET processed=true WHERE user_status_id='{}'"
                             .format(feed_table, result.get("user_status_id"))], fetch_result=False)
        except:
            logging.error("Error occurred while processing status id: {}".format(result.get("user_status_id")))


if __name__ == '__main__':
    process_new_tweets()

