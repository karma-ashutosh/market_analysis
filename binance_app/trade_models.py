import json
from enum import Enum


class TradeType(Enum):
    BUY = 1
    SELL = 2
    FAIL = 3


class TradeResult:
    def __init__(self, trade_type: TradeType, trade_price: float, quantity: float):
        self.trade_type = trade_type
        self.trade_price = trade_price
        self.quantity = quantity

    def __str__(self):
        j = {
            "type": self.trade_type.name,
            "price": self.trade_price,
            "quantity": self.quantity
        }
        return json.dumps(j)
