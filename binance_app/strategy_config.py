
class MovingAvgParams:
    SMALL = 'small_window'
    LARGE = 'large_window'

    params = {
        SMALL: 5,
        LARGE: 15
    }


class MACDParams:
    FAST = "period_fast"
    SLOW = "period_slow"
    SIGNAL = "signal"

    params = {
        FAST: 12,
        SLOW: 26,
        SIGNAL: 9
    }
