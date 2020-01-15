import json
from abc import abstractmethod
from logging import Logger

from kiteconnect import KiteConnect

from kite_enums import TransactionType

logger = None


def set_trade_execution_logger(target: Logger):
    global logger
    logger = target


class TradeExecutor:
    @abstractmethod
    def __execute_trade(self, trading_sym, market_event, transaction_type: TransactionType):
        pass

    def enter(self, trading_sym, market_event, transaction_type: TransactionType):
        self.__execute_trade(trading_sym, market_event, transaction_type)

    def exit(self, trading_sym, market_event, transaction_type: TransactionType):
        self.__execute_trade(trading_sym, market_event, transaction_type)


class KiteTradeExecutor(TradeExecutor):
    def __init__(self, kite_connect: KiteConnect):
        self._kite_connect = kite_connect

    def __execute_trade(self, trading_sym, market_event, transaction_type: TransactionType):
        try:
            if transaction_type == TransactionType.LONG:
                entry_price = market_event['depth']['sell'][0]['price']
            else:
                entry_price = market_event['depth']['buy'][0]['price']

            open_price = market_event['ohlc']['open']
            price_diff_percentage = (100 * abs(open_price - entry_price)) / open_price
            if entry_price > 1500 or price_diff_percentage > 5:
                logger.info("not executing trade in kite as entry_price was: {} and price_diff_percentage: {}"
                            .format(entry_price, price_diff_percentage))
            else:
                self.limit_order(entry_price, trading_sym, transaction_type)
        except:
            logger.error("error while executing order in kite for market event: {}".format(market_event))

    def market_order(self, price, trading_sym, transaction_type: TransactionType):
        kite_transaction_type = self.kite_transaction_type(transaction_type)
        self._kite_connect.place_order(
            variety=KiteConnect.VARIETY_REGULAR,
            exchange=KiteConnect.EXCHANGE_BSE,
            tradingsymbol=trading_sym,
            transaction_type=kite_transaction_type,
            quantity=1,
            product=KiteConnect.PRODUCT_MIS,
            order_type=KiteConnect.ORDER_TYPE_MARKET,
        )

    def limit_order(self, price, trading_sym, transaction_type: TransactionType):
        kite_transaction_type = self.kite_transaction_type(transaction_type)
        self._kite_connect.place_order(
            variety=KiteConnect.VARIETY_REGULAR,
            exchange=KiteConnect.EXCHANGE_BSE,
            tradingsymbol=trading_sym,
            transaction_type=kite_transaction_type,
            quantity=1,
            product=KiteConnect.PRODUCT_MIS,
            order_type=KiteConnect.ORDER_TYPE_LIMIT,
            price=price
        )

    def stop_loss_order(self, entry_price, trading_sym, transaction_type: TransactionType):
        kite_transaction_type = self.kite_transaction_type(transaction_type)

        self._kite_connect.place_order(
            variety=KiteConnect.VARIETY_REGULAR,
            exchange=KiteConnect.EXCHANGE_BSE,
            tradingsymbol=trading_sym,
            transaction_type=kite_transaction_type,
            quantity=1,
            product=KiteConnect.PRODUCT_MIS,
            order_type=KiteConnect.ORDER_TYPE_SLM,
            trigger_price=entry_price,

        )

    def bracket_order(self, entry_price, trading_sym, transaction_type: TransactionType):
        kite_transaction_type = self.kite_transaction_type(transaction_type)
        square_off = self.square_off_price(entry_price, transaction_type)
        stop_loss = entry_price * 0.015
        trailing_stop_loss = entry_price * 0.01
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
                    "price: {}".format(KiteConnect.VARIETY_BO, KiteConnect.EXCHANGE_NSE, trading_sym,
                                       kite_transaction_type, 1, KiteConnect.PRODUCT_MIS, KiteConnect.ORDER_TYPE_LIMIT,
                                       KiteConnect.VALIDITY_DAY, square_off, stop_loss, trailing_stop_loss,
                                       entry_price))
        self._kite_connect.place_order(variety=KiteConnect.VARIETY_BO,
                                       exchange=KiteConnect.EXCHANGE_NSE,
                                       tradingsymbol=trading_sym,
                                       transaction_type=kite_transaction_type,
                                       quantity=1,
                                       product=KiteConnect.PRODUCT_MIS,
                                       order_type=KiteConnect.ORDER_TYPE_LIMIT,
                                       validity=KiteConnect.VALIDITY_DAY,
                                       squareoff=square_off, stoploss=stop_loss,
                                       trailing_stoploss=trailing_stop_loss,
                                       price=entry_price)

    @staticmethod
    def square_off_price(entry_price, transaction_type: TransactionType) -> float:
        if transaction_type == TransactionType.LONG:
            square_off = entry_price * 1.05
        else:
            square_off = entry_price * 0.95
        return square_off

    @staticmethod
    def kite_transaction_type(transaction_type: TransactionType) -> str:
        if transaction_type == TransactionType.LONG:
            kite_transaction_type = KiteConnect.TRANSACTION_TYPE_BUY
        else:
            kite_transaction_type = KiteConnect.TRANSACTION_TYPE_SELL
        return kite_transaction_type


class DummyTradeExecutor(TradeExecutor):

    def __execute_trade(self, trading_sym, market_event, transaction_type: TransactionType):
        message = {
            'trade_executor': "DummyTradeExecutor",
            'trading_sym': trading_sym,
            'transaction_type': transaction_type.value,
            'market_event': str(market_event)
        }
        print("Executed trade: {}".format(json.dumps(message)))

    def exit(self, trading_sym, market_event, transaction_type: TransactionType):
        self.__execute_trade(trading_sym, market_event, transaction_type)
