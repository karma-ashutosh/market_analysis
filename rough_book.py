from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from general_util import get_all_values_for_key, file_to_dict_list, group_dict_array_by_key


def filter_stock_for_code(stock_arr, stock_code):
    return list(filter(lambda stock: stock['stock_code'] == stock_code, stock_arr))


def update_relevant_fields(j_elem):
    try:
        j_elem['averageprice'] = float(j_elem['averageprice'])
        j_elem['totaltradedvolume'] = float(j_elem['totaltradedvolume'])
        j_elem['query_time'] = datetime.strptime(j_elem['query_time'], '%Y-%m-%d %H:%M:%S.%f')
    except:
        pass


def plot_share_data(company_shares, date=None):
    if date:
        company_shares = filter_shares_for_date(company_shares, date)
    price = get_all_values_for_key(company_shares, 'averageprice')
    volume = get_all_values_for_key(company_shares, 'totaltradedvolume')
    time = get_all_values_for_key(company_shares, 'query_time')

    fig, ax1 = plt.subplots()

    ax2 = ax1.twinx()
    ax1.plot(time, price, 'g-')
    ax2.plot(time, volume, 'b-')

    ax1.set_xlabel('time data')
    ax1.set_ylabel('price data', color='g')
    ax2.set_ylabel('volume data', color='b')
    plt.show()


def filter_shares_for_date(company_shares, date):
    date = datetime.strptime(date + ' 00:00:00.0', '%Y-%m-%d %H:%M:%S.%f')
    next_day = date + timedelta(days=1)
    company_shares = list(filter(lambda entry: next_day > entry['query_time'] > date, company_shares))
    return company_shares


def get_dates_for_which_we_have_data(shares):
    result = set()
    for share in shares:
        time = share['query_time']
        result.add(
            "-".join([str(time.year).zfill(4), str(time.month).zfill(2), str(time.day).zfill(2)])
        )
    return list(result)


nano_count_dict = {}
def get_unique_nano_from_millis(millis):
    nano = millis * 1000000
    existing_count = nano_count_dict.get(nano, 0)
    nano_count_dict[nano] = existing_count + 1

    new_nano = nano + existing_count
    return new_nano


def get_influx_line(j_elem, machine_ip, thread_count, loop_count) -> str:
    tag_entries = ["machine_ip=" + str(machine_ip), "thread_count=" + str(thread_count),
                   "loop_count=" + str(loop_count)]

    field_entries = ["elapsed="+str(j_elem.get('elapsed'))]
    nano = get_unique_nano_from_millis(int(j_elem.get('timeStamp')))
    return "response_time," + ",".join(tag_entries) + " " + ",".join(field_entries) + " " + str(nano)


influx_file_header = """# DDL
CREATE DATABASE web_app_nfr

# DML
# CONTEXT-DATABASE: web_app_nfr

"""


def jtl_to_influx(source_file, destination_file, machine_ip, thread_count, loop_count):
    j_arr = file_to_dict_list(source_file)
    influx_lines = list(map(lambda j_elem: get_influx_line(j_elem, machine_ip, thread_count, loop_count), j_arr))
    f = open(destination_file, 'w')
    f.write(influx_file_header)
    f.write("\n".join(influx_lines))
    f.flush()
    f.close()


if __name__ == '__main__':
    # j_arr = file_to_dict_list("/Users/ashutosh.v/Downloads/stock_price_may_11.csv")
    j_arr = file_to_dict_list("/tmp/sample.csv")
    print("group")
    grouped_stocks = group_dict_array_by_key(lambda j_elem: j_elem['stock_code'], j_arr)
    plt.ion()
    while True:
        try:
            symbol = input("type share code (or type 1 to exit): ")
            if symbol == '1':
                break
            company_shares = grouped_stocks.get(symbol)

            if company_shares is None:
                print("There is no data for this share symbol, please provide another value")
                continue

            for entry in company_shares:
                update_relevant_fields(entry)

            dates = get_dates_for_which_we_have_data(company_shares)
            result_date = input("provide date for which you want to plot. Valid dates are: {} \nYour Selection: "
                                .format(dates))
            plot_share_data(company_shares, result_date)
        except Exception as e:
            print("Error occurred {}".format(e))
