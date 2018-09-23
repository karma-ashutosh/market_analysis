# import libraries
import urllib3
from bs4 import BeautifulSoup

# todo https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
http = urllib3.PoolManager()


def get_announcements_list() -> list:
    announcement_page = "https://www.bseindia.com/corporates/Forth_Results.aspx?expandable=0"
    page = http.request('GET', announcement_page)
    soup = BeautifulSoup(page.data, "html.parser")
    html_table = soup.find('table', attrs={'id': 'ctl00_ContentPlaceHolder1_gvData'})
    table_rows = list(map(lambda x: x.findAll("td"), html_table.findAll("tr")))
    visible_data = [[e.text for e in elements] for elements in table_rows][1:]  # ignoring the header column
    return visible_data


def get_announcement_result(security_code) -> list:
    security_result_url = "https://www.bseindia.com/corporates/ann.aspx?curpg=1&annflag=1&dt=&dur=A&dtto=" \
                          "&cat=Result&scrip={}&anntype=C".format(security_code)
    page = http.request('GET', security_result_url)
    soup = BeautifulSoup(page.data, "html.parser")
    html_table = soup.find('span', attrs={'id': 'ctl00_ContentPlaceHolder1_lblann'})
    rows = html_table.findAll("tr")
    links = rows[2].findAll('a', attrs={'class': 'tablebluelink'})
    publish_date = [header.text for header in rows[1].findAll('td', attrs={'class': 'announceheader'})]
    publish_date.extend([header.text for header in rows[2].findAll('td', attrs={'class': 'announceheader'})])
    publish_date_index = 0
    data_dict_list = []
    for i in range(len(links)):
        link = links[i]
        if link.attrs.get("href") and link.attrs.get("href").endswith(".pdf"):
            data_dict = {
                "title": links[i-1].text,
                "pdf_link": links[i]['href'],
                "date": publish_date[publish_date_index]
            }
            data_dict_list.append(data_dict)
            publish_date_index = publish_date_index + 1
    return data_dict_list


def download_file(download_url, write_path):
    response = http.urlopen(download_url)
    file = open(write_path, 'w')
    file.write(response.read())
    file.close()

