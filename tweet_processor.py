import re
import requests
from bs4 import BeautifulSoup
import json


def parse_url(text: str) -> str:
    re.search("(?P<url>https?://[^\s]+)", text).group("url")


def get_from_tiny(url):
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
    return json.dumps(company_details)
