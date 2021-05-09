import abc
import json
from datetime import datetime

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
    def buy(self, tick: MarketTickEntity, opportunity: Opportunity):
        pass

    @abc.abstractmethod
    def sell(self, tick: MarketTickEntity, opportunity: Opportunity):
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

    def buy(self, tick: MarketTickEntity, opp: Opportunity):
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
                return self.client.order_market_buy(symbol=self.symbol,
                                                    # side=Client.SIDE_BUY,
                                                    type=Client.ORDER_TYPE_MARKET,
                                                    quantity=quantity,
                                                    newOrderRespType=Client.ORDER_RESP_TYPE_FULL
                                                    )
            except BinanceAPIException as e:
                return self.handle_api_exception(e)

    def sell(self, tick: MarketTickEntity, opp: Opportunity):
        cur_price = tick.close
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


class AcademicTradeExecutor(TradeExecutor):
    class Trade:
        SYMBOL = "symbol"
        BUY = "buy_price"
        SELL = "sell_price"
        PNL = "profit_loss"
        TOTAL_STOCKS = "total_stocks"
        BUY_TIME = "buy_time"
        SELL_TIME = "sell_time"
        BUY_ATTRS = "buy_attrs"
        SELL_ATTRS = "sell_attrs"

        def __init__(self, symbol, buy_tick: MarketTickEntity, attrs: dict):
            self.symbol = symbol
            self.trading_fee_percentage = 0.1
            self.total_amount = 10
            self.effective_amount = self.__amount_left_after_fee(self.total_amount)

            self.buy_price = buy_tick.close
            self.buy_time = buy_tick.window_end_epoch_seconds
            self.buy_attrs = attrs

            self.total_stocks = self.effective_amount / self.buy_price

            self.sell_price = None
            self.sell_time = None
            self.sell_attrs = None

            self.profit_or_loss = None
            self.profit_or_loss_with_fee = None

        def __amount_left_after_fee(self, amount):
            effective_amount = amount * (1 - self.trading_fee_percentage / 100)
            # ((100 + trading_fee_percentage) / 100) * effective_amount = amount
            return effective_amount

        def sell_at(self, tick: MarketTickEntity, attrs: dict):
            self.sell_attrs = attrs
            self.sell_price = tick.close
            self.sell_time = tick.window_end_epoch_seconds
            amount_returned = self.sell_price * self.total_stocks
            after_fee = self.__amount_left_after_fee(amount_returned)
            self.profit_or_loss = after_fee - self.total_amount

        def to_json(self):
            result = {
                AcademicTradeExecutor.Trade.SYMBOL: self.symbol,
                AcademicTradeExecutor.Trade.BUY: self.buy_price,
                AcademicTradeExecutor.Trade.SELL: self.sell_price,
                AcademicTradeExecutor.Trade.PNL: self.profit_or_loss,
                AcademicTradeExecutor.Trade.TOTAL_STOCKS: self.total_stocks,
                AcademicTradeExecutor.Trade.BUY_TIME: datetime.fromtimestamp(self.buy_time / 1000).strftime("%m/%d/%Y, %H:%M:%S"),
                AcademicTradeExecutor.Trade.SELL_TIME: datetime.fromtimestamp(self.sell_time / 1000).strftime("%m/%d/%Y, %H:%M:%S"),
                AcademicTradeExecutor.Trade.BUY_ATTRS: self.buy_attrs,
                AcademicTradeExecutor.Trade.SELL_ATTRS: self.sell_attrs,
            }
            return result

        def __str__(self):
            result = self.to_json
            return json.dumps(result, indent=1)

    def __init__(self, symbol):
        super().__init__(symbol)
        self.__trades = []
        self.__cur_trade: AcademicTradeExecutor.Trade = None

    def buy(self, tick: MarketTickEntity, opp: Opportunity):
        self.__cur_trade = AcademicTradeExecutor.Trade(self.symbol, tick, opp.attrs)
        app_logger.info("Buying {} at price {}".format(self.symbol, tick.close))

    def sell(self, tick: MarketTickEntity, opp: Opportunity):
        self.__cur_trade.sell_at(tick, opp.attrs)
        self.__trades.append(self.__cur_trade)
        self.__cur_trade = None
        app_logger.info("Selling {} at price {}".format(self.symbol, tick.close))

    def get_all_trades(self):
        print("total trade size: {}".format(len(self.__trades)))
        return [trade.to_json() for trade in self.__trades]
