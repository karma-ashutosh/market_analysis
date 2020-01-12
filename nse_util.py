import requests


class NSEHistoricalDataCrawler:
    def __init__(self):
        pass

    def parse(self, symbol):
        url = "https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?" \
              "symbol={}&" \
              "segmentLink=3&" \
              "symbolCount=1&" \
              "series=EQ&" \
              "dateRange=1month&" \
              "fromDate=&toDate=&dataType=PRICEVOLUMEDELIVERABLE".format(symbol)
        r = requests.get(url)

