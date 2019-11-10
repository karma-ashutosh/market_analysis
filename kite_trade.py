from kiteconnect import KiteConnect
from kite_enums import Variety, Exchange, TransactionType, PRODUCT, OrderType, VALIDITY
"""
   def place_order(self,
                    variety,
                    exchange,
                    tradingsymbol,
                    transaction_type,
                    quantity,
                    product,
                    order_type,
                    price=None,
                    validity=None,
                    disclosed_quantity=None,
                    trigger_price=None,
                    squareoff=None,
                    stoploss=None,
                    trailing_stoploss=None,
                    tag=None):"""


def place_order(kite: KiteConnect, trading_symbol):
    kite.place_order(variety=Variety.BRACKET.value, exchange=Exchange.BSE.value, tradingsymbol=trading_symbol,
                     transaction_type=TransactionType.BUY.value, quantity=1, product=PRODUCT.MIS.value,
                     order_type=OrderType.MARKET.value, validity=VALIDITY.DAY.value, squareoff=3.0, stoploss=2.0,
                     trailing_stoploss=1.0, price=70)

    kite.place_order(variety=Variety.BRACKET.value,
                     exchange=Exchange.NSE.value,
                     tradingsymbol='INFY',
                     transaction_type=TransactionType.BUY.value,
                     quantity=1,
                     product=PRODUCT.MIS.value,
                     order_type=OrderType.LIMIT.value,
                     validity=VALIDITY.DAY.value,
                     squareoff=3.0, stoploss=2.0, trailing_stoploss=1.0, price=627)
    pass
