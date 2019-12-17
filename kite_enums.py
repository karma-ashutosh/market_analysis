from enum import Enum




class MultiEnum(Enum):
    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, description: str = None):
        self._description_ = description

    def __str__(self):
        return self.value

    # this makes sure that the description is read-only
    @property
    def description(self):
        return self._description_


class KiteVariety(MultiEnum):
    REGULAR = "regular", "Regular Order"
    AFTER_MARKET = "amo","After Market Order"
    BRACKET = "bo", "Bracket Order"
    COVER = "co", "Cover Order"


class KiteOrderType(MultiEnum):
    MARKET = "MARKET", "Market Order"
    LIMIT = "LIMIT", "Limit Order"
    STOPLOSS = "SL", "StopLoss Order"
    STOPLOSS_MARKET = "SL-M", "StopLoss Market Order"


class KitePRODUCT(MultiEnum):
    CNC = "CNC", "CASH N CARRY for equity"
    NORMAL = "NRML", "Normal for futures and options"
    MIS = "MIS", "Margin Intraday Squareoff for futures and options"


class KiteVALIDITY(MultiEnum):
    DAY = "DAY", "REGULAR ORDER"
    IOC = "IOC", "Immediate or Cancel"


class KiteBracketOrderType(MultiEnum):
    SQUARE_OFF = "squareoff", "Price difference at which the order should be squared off and profit booked (eg: Order " \
                              "price is 100. Profit target is 102. So squareoff = 2) "
    STOP_LOSS = "stoploss", "Stoploss difference at which the order should be squared off (eg: Order price is 100. " \
                            "Stoploss target is 98. So stoploss = 2) "
    TRAILING_STOP_LOSS = "trailing_stoploss", "Incremental value by which stoploss price changes when market moves in " \
                                              "your favor by the same incremental value from the entry price (" \
                                              "optional) "


class KiteExchange(Enum):
    BSE = "BSE"
    NSE = "NSE"


class TransactionType(Enum):
    SHORT = 1
    LONG = 2


