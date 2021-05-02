from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.websockets import BinanceSocketManager
from requests.models import PreparedRequest
from constants import BINANCE


def format_prepped_request(prepped: PreparedRequest):
    print("type: {}".format(type(prepped)))
    # prepped has .method, .path_url, .headers and .body attribute to view the request
    body = prepped.body
    headers = '\n'.join(['{}: {}'.format(*hv) for hv in prepped.headers.items()])
    return f"""\
{prepped.method} {prepped.path_url} HTTP/1.1
{headers}

{prepped.url}
{prepped.body.format()}

{body}\n\n"""


class ParsedBinanceAPIException:
    def __init__(self, e: BinanceAPIException):
        self.status_code = e.status_code
        self.response = e.response
        self.binance_code = e.code
        self.error_message = e.message
        self.request = e.request


class TradeExecutor:
    def __init__(self, client: Client, symbol):
        self.client = client
        self.symbol = symbol
        self.min_usdt_to_spend = 10.5

    def handle_api_exception(self, e: BinanceAPIException):
        parsed = ParsedBinanceAPIException(e)
        print("status: \t{}\nbinance_code: \t{}\nerror_message: \t{}\nrequest: \t{}\nresponse: \t{}\n"
              .format(parsed.status_code, parsed.binance_code, parsed.error_message, parsed.request, parsed.response.text))
        print("request: {}".format(format_prepped_request(parsed.request)))
        return parsed

    def buy(self, cur_price):
        try:
            quantity = self.min_usdt_to_spend / cur_price
            print("{}\t buying {} quantity at cur_price: {}".format(self.symbol, quantity, cur_price))
            return self.client.order_market_buy(symbol=self.symbol,
                                                # side=Client.SIDE_BUY,
                                                type=Client.ORDER_TYPE_MARKET,
                                                quantity=quantity,
                                                newOrderRespType=Client.ORDER_RESP_TYPE_FULL
                                                )
        except BinanceAPIException as e:
            return self.handle_api_exception(e)

    def sell(self, cur_price):
        try:
            quantity = self.min_usdt_to_spend / cur_price
            print("{}\t selling {} quantity at cur_price: {}".format(self.symbol, quantity, cur_price))
            return self.client.order_market_sell(symbol=self.symbol,
                                                 # side=Client.SIDE_SELL,
                                                 type=Client.ORDER_TYPE_MARKET,
                                                 quantity=quantity,
                                                 newOrderRespType=Client.ORDER_RESP_TYPE_FULL
                                                 )
        except BinanceAPIException as e:
            return self.handle_api_exception(e)


class Factory:
    def __init__(self):
        self.client = Client(BINANCE.API_KEY, BINANCE.SECRET_KEU)

    def open_kline_connection(self, processor=lambda msg: print(msg), symbol=BINANCE.SYMBOL):
        bm = BinanceSocketManager(self.client)
        bm.start_kline_socket(symbol, processor, interval=Client.KLINE_INTERVAL_1MINUTE)
        bm.start()

    def trade_executor(self, symbol) -> TradeExecutor:
        return TradeExecutor(self.client, symbol)
