from kiteconnect import KiteConnect
from kite_enums import Variety, Exchange, TransactionType
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


def place_order_by_money(kite: KiteConnect, instrument_code, amount):
    kite.place_order(variety=Variety.BRACKET.value, exchange=Exchange.BSE.value, tradingsymbol=instrument_code,
                     transaction_type=TransactionType.BUY.value,   )
    pass
