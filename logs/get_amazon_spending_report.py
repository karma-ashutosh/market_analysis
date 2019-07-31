from bs4 import BeautifulSoup


def get_amount_spent_in_file(file_path):
    text = open(file_path).read()
    soup = BeautifulSoup(text, "html.parser")
    order_details = soup.findAll('div', attrs={'class': 'a-box-group a-spacing-base order'})
    return list(map(get_amount_from_order, order_details))


def get_amount_from_order(order_detail):
    try:
        head = order_detail.findAll('div', attrs={'class': 'a-box a-color-offset-background order-info'})
        h = str(head)
        return float(h.split("Rs. </span>")[1].split("</span>")[0].replace(",", ""))
        # amount_tag = head.find('div', attrs={'class': 'a-column a-span2'})
        # amount = amount_tag.find('span', attrs={'style': 'text-decoration: inherit; white-space: nowrap;'}).getText()
        # return amount
    except:
        return ''


def total_spent_in_year(year, num_of_files):
    num = list(range(1, num_of_files + 1))
    file_names = ["Page{}.htm".format(n) for n in num]
    base_dir = "/Users/ashutosh.v/Desktop/AmazonOrders/{}/".format(year)
    file_paths = [base_dir + name for name in file_names]
    spent_by_file = [get_amount_spent_in_file(file_path) for file_path in file_paths]
    flat_spent = []
    for spent_amount in spent_by_file:
        flat_spent.extend(spent_amount)
    print(sum(flat_spent))


def total_spent_2018():
    year = 2018
    total_spent_in_year(year, 11)


def total_spent_2019():
    year = 2019
    total_spent_in_year(year, 5)


if __name__ == '__main__':
    total_spent_2018()
    total_spent_2019()
