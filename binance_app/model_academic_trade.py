import json
from datetime import datetime

from market_tick import MarketTickEntity


class LongTrade:
    SYMBOL = "symbol"
    BUY = "buy_price"
    SELL = "sell_price"
    PNL = "profit_loss"
    TOTAL_STOCKS = "total_stocks"
    BUY_TIME = "buy_time"
    SELL_TIME = "sell_time"
    BUY_ATTRS = "buy_attrs"
    SELL_ATTRS = "sell_attrs"
    TRADE_TYPE = "trade_type"

    def __init__(self, symbol, buy_tick: MarketTickEntity, attrs: dict, money, price, trading_fee_percentage):
        self.symbol = symbol
        self.trading_fee_percentage = trading_fee_percentage
        self.total_amount = money
        self.trading_fee_paid = 0

        self.effective_amount = self.__amount_left_after_fee(self.total_amount)
        self.trading_fee_paid = self.trading_fee_paid + (self.total_amount - self.effective_amount)

        self.buy_price = price
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
        return effective_amount

    def sell_at(self, tick: MarketTickEntity, attrs: dict, price):
        self.sell_attrs = attrs
        self.sell_price = price
        self.sell_time = tick.window_end_epoch_seconds
        amount_returned = self.sell_price * self.total_stocks
        after_fee = self.__amount_left_after_fee(amount_returned)
        self.trading_fee_paid = self.trading_fee_paid + (amount_returned - after_fee)
        self.profit_or_loss = after_fee - self.total_amount
        margin_percentage = (self.profit_or_loss / self.total_amount) * 100
        fpr = '.3f'
        # print("Stock : {}\tBuy Date: {}\tBuy Price: {}\tSell Price: {}\tsold_early: {}\tmargin: {}\tmargin_percentage: {}, trading_fee: {}\n\n"
        #       .format(self.symbol,
        #               str(datetime.fromtimestamp(self.buy_time)),
        #               format(self.buy_price, fpr),
        #               format(self.sell_price, fpr), price is not None,
        #               format(self.profit_or_loss, fpr),
        #               format(margin_percentage, fpr),
        #               format(self.trading_fee_paid, fpr)))

    @staticmethod
    def try_parse_time(source):
        try:
            return datetime.fromtimestamp(source).strftime("%m/%d/%Y, %H:%M:%S").replace(',', '-') if source else None
        except:
            try:
                return datetime.fromtimestamp(source / 1000).strftime("%m/%d/%Y, %H:%M:%S").replace(',', '-') if source else None
            except Exception as e:
                raise e

    def to_json(self):
        result = {
            LongTrade.SYMBOL: self.symbol,
            LongTrade.BUY: self.buy_price,
            LongTrade.SELL: self.sell_price,
            LongTrade.PNL: self.profit_or_loss,
            LongTrade.TOTAL_STOCKS: self.total_stocks,
            LongTrade.BUY_TIME: self.try_parse_time(self.buy_time) if self.buy_time else None,
            LongTrade.SELL_TIME: self.try_parse_time(self.sell_time) if self.sell_time else None,
            LongTrade.BUY_ATTRS: self.buy_attrs,
            LongTrade.SELL_ATTRS: self.sell_attrs,
            LongTrade.TRADE_TYPE: "LONG"
        }
        return result

    def __str__(self):
        result = self.to_json()
        return json.dumps(result, indent=1)


class ShortTrade:
    def __init__(self, symbol, sell_tick: MarketTickEntity, attrs: dict, money, price, trading_fee_per):
        self.symbol = symbol
        self.trading_fee_percentage = trading_fee_per
        self.total_amount = money
        self.effective_amount = self.__amount_left_after_fee(self.total_amount)

        self.sell_price = price
        self.sell_time = sell_tick.window_end_epoch_seconds
        self.sell_attrs = attrs
        self.total_stocks = self.effective_amount / self.sell_price

        self.buy_price = None
        self.buy_time = None
        self.buy_attrs = None

        self.profit_or_loss = None
        self.profit_or_loss_with_fee = None

    def __amount_left_after_fee(self, amount):
        # lending interest is not taken into account
        effective_amount = amount * (1 - self.trading_fee_percentage / 100)
        return effective_amount

    def __amount_needed_for_after_fee_amount(self, amount):
        needed = amount / (1 - self.trading_fee_percentage / 100)
        return needed

    def buy_at(self, tick: MarketTickEntity, attrs: dict, price):
        self.buy_attrs = attrs
        self.buy_price = price
        self.buy_time = tick.window_end_epoch_seconds
        buy_amount = self.buy_price * self.total_stocks
        amount_spent = self.__amount_needed_for_after_fee_amount(buy_amount)
        self.profit_or_loss = self.total_amount - amount_spent

    def to_json(self):
        result = {
            LongTrade.SYMBOL: self.symbol,
            LongTrade.BUY: self.buy_price,
            LongTrade.SELL: self.sell_price,
            LongTrade.PNL: self.profit_or_loss,
            LongTrade.TOTAL_STOCKS: self.total_stocks,
            LongTrade.BUY_TIME: LongTrade.try_parse_time(self.buy_time) if self.buy_time else None,
            LongTrade.SELL_TIME: LongTrade.try_parse_time(self.sell_time) if self.sell_time else None,
            LongTrade.BUY_ATTRS: self.buy_attrs,
            LongTrade.SELL_ATTRS: self.sell_attrs,
            LongTrade.TRADE_TYPE: "SHORT"
        }
        return result

    def __str__(self):
        result = self.to_json
        return json.dumps(result, indent=1)
