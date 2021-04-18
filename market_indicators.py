from kite_data_downloader import Nify50LastNDaysDownloader

class MovingAvgIndicator:
    def __init__(self):
        pass

    def generate_indicators(self):
        pass

    def flush_indicators(self):
        pass


if __name__ == '__main__':
    Nify50LastNDaysDownloader(number_of_days=60).download()

