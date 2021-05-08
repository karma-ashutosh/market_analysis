from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance.websockets import BinanceSocketManager
from requests.models import PreparedRequest
from constants import BINANCE
from util_general import app_logger


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

\n\n"""


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
        self.min_usdt_to_spend = 15

    @staticmethod
    def handle_api_exception(e: BinanceAPIException):
        parsed = ParsedBinanceAPIException(e)
        app_logger.error("status: \t{}\nbinance_code: \t{}\nerror_message: \t{}\nrequest: \t{}\nresponse: \t{}\n"
                         .format(parsed.status_code, parsed.binance_code, parsed.error_message, parsed.request,
                                 parsed.response.text))
        app_logger.error("request: {}".format(format_prepped_request(parsed.request)))
        return parsed

    def buy(self, cur_price):
        holding = self.__tradable_held_quantity()
        total_worth = holding * cur_price
        if int(total_worth) > self.min_usdt_to_spend:
            app_logger.info("Already have {} units in account at total worth {}. Skipping purchase order. "
                            .format(holding, total_worth))
        else:
            try:
                quantity = int(self.min_usdt_to_spend / cur_price) + 1
                app_logger.info("{}\t buying {} quantity at cur_price: {}".format(self.symbol, quantity, cur_price))
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
            quantity = self.__tradable_held_quantity()
            app_logger.info("{}\t selling {} quantity at cur_price: {}".format(self.symbol, quantity, cur_price))
            return self.client.order_market_sell(symbol=self.symbol,
                                                 # side=Client.SIDE_SELL,
                                                 type=Client.ORDER_TYPE_MARKET,
                                                 quantity=quantity,
                                                 newOrderRespType=Client.ORDER_RESP_TYPE_FULL
                                                 )
        except BinanceAPIException as e:
            return self.handle_api_exception(e)

    def __tradable_held_quantity(self):
        account = self.client.get_account()
        # account['balances'] = [{'asset': 'USDT', 'free': '37.98910089', 'locked': '0.00000000'}.....]
        coin_details = next(filter(lambda x: x['asset'] == BINANCE.COIN, account['balances']))
        return int(float(coin_details['free']))


class Factory:
    # https://github.com/binance/binance-public-data/
    def __init__(self):
        self.client = Client(BINANCE.API_KEY, BINANCE.SECRET_KEU)

    def open_kline_connection(self, processor=lambda msg: print(msg), symbol=BINANCE.SYMBOL):
        bm = BinanceSocketManager(self.client)
        bm.start_kline_socket(symbol, processor, interval=Client.KLINE_INTERVAL_1MINUTE)
        bm.start()

    def trade_executor(self, symbol) -> TradeExecutor:
        return TradeExecutor(self.client, symbol)


if __name__ == '__main__':
    provider = Factory()
    provider.trade_executor(BINANCE.SYMBOL).sell(.38)
