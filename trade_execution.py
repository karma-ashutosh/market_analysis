import json
from abc import abstractmethod
from logging import Logger

from kiteconnect import KiteConnect

from kite_enums import KiteVariety, KiteExchange, KitePRODUCT, KiteOrderType, KiteVALIDITY, TransactionType

logger = None


def set_trade_execution_logger(target: Logger):
    global logger
    logger = target


class TradeExecutor:
    @abstractmethod
    def execute_trade(self, trading_sym, market_event, transaction_type: TransactionType):
        pass


class KiteTradeExecutor(TradeExecutor):
    def __init__(self, kite_connect: KiteConnect):
        self._kite_connect = kite_connect

    def execute_trade(self, trading_sym, market_event, transaction_type: TransactionType):
        try:
            if transaction_type == TransactionType.LONG:
                entry_price = market_event['depth']['sell'][0]['price']
                kite_transaction_type = "BUY"
                square_off = entry_price * 1.05
            else:
                entry_price = market_event['depth']['buy'][0]['price']
                kite_transaction_type = "SELL"
                square_off = entry_price * 0.95
            stop_loss = entry_price * 0.015
            trailing_stop_loss = entry_price * 0.01

            open_price = market_event['ohlc']['open']
            price_diff_percentage = (100 * abs(open_price - entry_price)) / open_price
            if entry_price > 1500 or price_diff_percentage > 5:
                logger.info("not executing trade in kite as entry_price was: {} and price_diff_percentage: {}"
                            .format(entry_price, price_diff_percentage))
            else:
                logger.info("Executing trade with params: "
                            "variety: {}, "
                            "exchange: {}, "
                            "tradingsymbol: {}, "
                            "transaction_type: {}, "
                            "quantity: {}, "
                            "product: {}, "
                            "order_type: {}, "
                            "validity: {}, "
                            "squareoff: {}, "
                            "stoploss: {} "
                            "trailing_stoploss: {}, "
                            "price: {}".format(KiteVariety.BRACKET.value, KiteExchange.NSE.value, trading_sym,
                                               kite_transaction_type, 1, KitePRODUCT.MIS.value, KiteOrderType.LIMIT.value,
                                               KiteVALIDITY.DAY.value, square_off, stop_loss, trailing_stop_loss,
                                               entry_price))
                self._kite_connect.place_order(variety=KiteVariety.BRACKET.value,
                                               exchange=KiteExchange.NSE.value,
                                               tradingsymbol=trading_sym,
                                               transaction_type=kite_transaction_type,
                                               quantity=1,
                                               product=KitePRODUCT.MIS.value,
                                               order_type=KiteOrderType.LIMIT.value,
                                               validity=KiteVALIDITY.DAY.value,
                                               squareoff=square_off, stoploss=stop_loss,
                                               trailing_stoploss=trailing_stop_loss,
                                               price=entry_price)
        except:
            logger.error("error while executing order in kite for market event: {}".format(market_event))


class DummyTradeExecutor(TradeExecutor):

    def execute_trade(self, trading_sym, market_event, transaction_type: TransactionType):
        message = {
            'trade_executor': "DummyTradeExecutor",
            'trading_sym': trading_sym,
            'transaction_type': transaction_type.value,
            'market_event': str(market_event)
        }
        print("Executed trade: {}".format(json.dumps(message)))
