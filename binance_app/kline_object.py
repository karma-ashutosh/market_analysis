class KLineEntity:
    # to see the full list of parameters in k_line, visit
    # https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams
    def __init__(self, j_elem: dict):
        self.event = j_elem

        self.event_type = j_elem.get('e')
        self.event_time = j_elem.get('E')
        self.symbol = j_elem.get('s')
        data = j_elem.get('k')
        self.window_start_epoch_seconds = data.get('t')
        self.window_end_epoch_seconds = int(data.get('T'))
        self.interval = data.get('i')
        self.open = float(data.get('o'))
        self.close = float(data.get('c'))
        self.high = float(data.get('h'))
        self.low = float(data.get('l'))
        self.volume = float(data.get('v'))
        self.trade_count = data.get('n')

    def raw_json(self) -> dict:
        return self.event


def map_historical_k_line(historical_api_k_line):
    d = {
        'e': 'historical',
        'k': {
            't': historical_api_k_line[0],
            'o': historical_api_k_line[1],
            'h': historical_api_k_line[2],
            'l': historical_api_k_line[3],
            'c': historical_api_k_line[4],
            'v': historical_api_k_line[5],
            'T': historical_api_k_line[6],
            # number 7 -> Quote asset volume
            'n': historical_api_k_line[8],
            # number 9 -> Taker buy base asset volume
            # number 10 -> Taker buy quote asset volume
            # 11 -> ignore

        }
    }
    print(d)
    return KLineEntity(d)
