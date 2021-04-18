from kite_data_downloader import Nify50LastNDaysDownloader
from constants import TextFileConstants
from moving_avg_simulator import CombinedCrossOverGenerator
from datetime import datetime, timedelta
from general_util import save_csv_and_json_output


class MovingAvgIndicator:
    def __init__(self, past_days, smaller_window, larger_window):
        self.larger_window = larger_window
        self.smaller_window = smaller_window
        self.past_days = past_days
        self.file_name, self.filter_func = self.__variables()
        self.result_cross_overs = None

    def __variables(self):
        today = datetime.today()
        start_date = today - timedelta(days=self.past_days)
        filter_smaller_than = str(start_date.date())
        file_name = "cross_overs_{}_to_{}".format(filter_smaller_than, str(today.date()))

        def filter_func(cross_over):
            return cross_over.date >= filter_smaller_than

        return file_name, filter_func

    def generate_indicators(self):
        combined_cross_overs = CombinedCrossOverGenerator.get_combined_cross_overs(TextFileConstants.KITE_CURRENT_DATA,
                                                                                   small_window=self.smaller_window,
                                                                                   large_winodow=self.larger_window)
        required_cross_overs = list(filter(self.filter_func, combined_cross_overs))
        self.result_cross_overs = [cross_over.json() for cross_over in required_cross_overs]
        return self

    def flush_indicators(self):
        save_csv_and_json_output(self.result_cross_overs, TextFileConstants.KITE_DATA_BASE_DIR + self.file_name)


if __name__ == '__main__':
    # Nify50LastNDaysDownloader(number_of_days=60).download()
    MovingAvgIndicator(past_days=0, smaller_window=1, larger_window=5).generate_indicators().flush_indicators()
