class KLineEntity:
    # to see the full list of parameters in k_line, visit
    # https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams
    def __init__(self, j_elem: dict):
        self.event = j_elem

        self.event_type = j_elem['e']
        self.event_time = j_elem['E']
        self.symbol = j_elem['s']
        data = j_elem['k']
        self.window_start_epoch_seconds = data['t']
        self.window_end_epoch_seconds = data['T']
        self.interval = data['i']
        self.open = data['o']
        self.close = data['c']
        self.high = data['h']
        self.low = data['l']
        self.volume = data['v']
        self.trade_count = data['n']

    def raw_json(self) -> dict:
        return self.event
