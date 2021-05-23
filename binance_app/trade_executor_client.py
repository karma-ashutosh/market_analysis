import abc
from model_academic_trade import LongTrade, ShortTrade
from trade_models import TradeResult, TradeType
from market_tick import MarketTickEntity
from binance.client import Client
from binance.exceptions import BinanceAPIException
from requests.models import PreparedRequest
from analyzer_models import Opportunity
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
    def __init__(self, symbol):
        self.symbol = symbol

    @abc.abstractmethod
    def buy(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        pass

    @abc.abstractmethod
    def sell(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        pass

    @abc.abstractmethod
    def take_short(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        pass

    @abc.abstractmethod
    def square_short(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        pass


class BinanceTradeExecutor(TradeExecutor):
    def __init__(self, client: Client, symbol):
        super().__init__(symbol)
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

    def buy(self, tick: MarketTickEntity, opp: Opportunity, price) -> TradeResult:
        cur_price = tick.close
        holding = self.__tradable_held_quantity()
        total_worth = holding * cur_price
        if int(total_worth) > self.min_usdt_to_spend:
            app_logger.info("Already have {} units in account at total worth {}. Skipping purchase order. "
                            .format(holding, total_worth))
        else:
            try:
                quantity = int(self.min_usdt_to_spend / cur_price) + 1
                app_logger.info("{}\t buying {} quantity at cur_price: {}".format(self.symbol, quantity, cur_price))
                self.client.order_market_buy(symbol=self.symbol,
                                             # side=Client.SIDE_BUY,
                                             type=Client.ORDER_TYPE_MARKET,
                                             quantity=quantity,
                                             newOrderRespType=Client.ORDER_RESP_TYPE_FULL)
                return TradeResult(TradeType.BUY, cur_price, quantity)
            except BinanceAPIException as e:
                app_logger.error(self.handle_api_exception(e))
                return TradeResult(TradeType.FAIL, 0, 0)

    def sell(self, tick: MarketTickEntity, opp: Opportunity, price) -> TradeResult:

        cur_price = tick.close
        try:
            quantity = self.__tradable_held_quantity()
            app_logger.info("{}\t selling {} quantity at cur_price: {}".format(self.symbol, quantity, cur_price))
            self.client.order_market_sell(symbol=self.symbol,
                                          # side=Client.SIDE_SELL,
                                          type=Client.ORDER_TYPE_MARKET,
                                          quantity=quantity,
                                          newOrderRespType=Client.ORDER_RESP_TYPE_FULL
                                          )
            return TradeResult(TradeType.SELL, cur_price, quantity)
        except BinanceAPIException as e:
            app_logger.error(self.handle_api_exception(e))
            return TradeResult(TradeType.FAIL, 0, 0)

    def square_short(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        pass

    def take_short(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        pass

    def __tradable_held_quantity(self):
        account = self.client.get_account()
        # account['balances'] = [{'asset': 'USDT', 'free': '37.98910089', 'locked': '0.00000000'}.....]
        coin_details = next(filter(lambda x: x['asset'] == BINANCE.COIN, account['balances']))
        return int(float(coin_details['free']))


class AcademicTradeExecutor(TradeExecutor):
    def __init__(self, symbol, money):
        super().__init__(symbol)
        self.__long_trades = []
        self.__short_trades = []
        self.__cur_long_trade: LongTrade = None
        self.__cur_short_trade: ShortTrade = None
        self.money = money

    def buy(self, tick: MarketTickEntity, opp: Opportunity, price) -> TradeResult:
        if not self.__cur_long_trade:
            self.__cur_long_trade = LongTrade(self.symbol, tick, opp.attrs, self.money, price=price)
            app_logger.info("Buying {} at price {}".format(self.symbol, tick.close if not price else price))
            return TradeResult(TradeType.BUY, self.__cur_long_trade.buy_price, self.__cur_long_trade.total_stocks)

    def sell(self, tick: MarketTickEntity, opp: Opportunity, price) -> TradeResult:
        # print("Selling and closing position with {}".format(self.__cur_long_trade))
        if self.__cur_long_trade is not None:
            self.__cur_long_trade.sell_at(tick, opp.attrs, price=price)
            self.__long_trades.append(self.__cur_long_trade)
            app_logger.info("Selling {} at price {}".format(self.symbol, tick.close if not price else price))

            result = TradeResult(TradeType.SELL, self.__cur_long_trade.sell_price, self.__cur_long_trade.total_stocks)
            self.__cur_long_trade = None
            return result

    def take_short(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        if not self.__cur_short_trade:
            self.__cur_short_trade = ShortTrade(self.symbol, tick, opportunity.attrs, self.money, price)
            app_logger.info("Shorting {} at price {}".format(self.symbol, tick.close if not price else price))
            return TradeResult(TradeType.SELL, self.__cur_short_trade.sell_price, self.__cur_short_trade.total_stocks)

    def square_short(self, tick: MarketTickEntity, opportunity: Opportunity, price) -> TradeResult:
        if self.__cur_short_trade:
            self.__cur_short_trade.buy_at(tick, attrs=opportunity.attrs, price=price)
            self.__short_trades.append(self.__cur_short_trade)

            result = TradeResult(TradeType.BUY, self.__cur_short_trade.buy_price, self.__cur_short_trade.total_stocks)
            self.__cur_short_trade = None
            return result

    def get_all_trades(self):
        print("total trade size: {}".format(len(self.__long_trades)))
        longs = [trade.to_json() for trade in filter(lambda trade: trade.buy_price and trade.sell_price,
                                                     self.__long_trades)]
        shorts = [trade.to_json() for trade in filter(lambda trade: trade.buy_price and trade.sell_price,
                                                      self.__short_trades)]

        return longs, shorts

