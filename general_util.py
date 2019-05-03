import concurrent.futures
import errno
import gzip
import json
import logging
import os
import smtplib
import subprocess
import threading
from calendar import timegm
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from os.path import basename
from time import sleep

import yaml

with open('./config.yml') as handle:
    config_dict = yaml.load(handle)


def config_section_map(section: str):
    return config_dict.get(section, {})


def update_config(config_diff):
    return overwrite_or_merge(config_dict, config_diff)


def get_config_copy():
    return config_dict.copy()


def overwrite_or_merge(target_dict: dict, source_dict: dict, path=None):
    "merges b into a"
    if path is None:
        path = []

    for key in source_dict:
        if key in target_dict:
            if isinstance(target_dict[key], dict) and isinstance(source_dict[key], dict):
                overwrite_or_merge(target_dict[key], source_dict[key], path + [str(key)])
            else:
                target_dict[key] = source_dict[key]  # overwriting value in a by value in b
        else:
            target_dict[key] = source_dict[key]
    return target_dict


formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
msg_only_formatter = logging.Formatter('%(message)s')


def setup_logger(name, log_file, level=logging.INFO, logging_format=True, msg_only=False, log_rotation_unit='h',
                 log_rotation_interval=4):
    from logging import handlers
    """Function setup as many loggers as you want"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    # handler = logging.FileHandler(log_file)
    handler = handlers.TimedRotatingFileHandler(log_file, when=log_rotation_unit, interval=log_rotation_interval)
    if logging_format:
        if msg_only:
            handler.setFormatter(msg_only_formatter)
        else:
            handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def delete_file_silently(file_path):
    try:
        os.remove(file_path)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT: no such file or directory
            raise e


def list_hdfs_files_in_directory(directory: str) -> list:
    command = "hdfs dfs -ls {}".format(directory)
    lines = execute_command_and_get_console_output(command)
    lines_with_dir = [x for x in lines if directory in x]
    return [line.rsplit(" ", 1)[1] for line in lines_with_dir]


def execute_command_and_get_console_output(command: str) -> list:
    command = command.split(" ")
    stdout = str(subprocess.Popen(command, stdout=subprocess.PIPE).stdout.read())
    lines = stdout.split("\\n")
    return lines


def get_yarn_application_status(application_id: str) -> dict:
    command = "yarn application -status {}".format(application_id).split(" ")
    stdout = str(subprocess.Popen(command, stdout=subprocess.PIPE).stdout.read())
    lines = [line.replace("\\t", "") for line in stdout.split("\\n")]
    d = {}
    for line in lines:
        if 'State' in line or 'Tracking-URL' in line or 'Application-Id' in line or 'Final-State' in line:
            key, val = line.split(" : ")
            d[key] = val
    return d


def run_in_background(job, args=()):
    threading.Thread(target=job, args=args).start()


def gunzip(obj):
    decompressed = gzip.decompress(obj)
    txt = decompressed.decode('utf-8')
    lines = txt.split("\n")
    return lines


def send_mail(username: str, password: str, subject: str, body: str, to_addrs: list, attachments=None):
    print("sending mail")
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login(username, password)

    msg = MIMEMultipart()
    msg['From'] = username
    msg['To'] = COMMASPACE.join(to_addrs)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    msg.attach(MIMEText(body))

    for f in attachments or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    server.sendmail(username, to_addrs, msg.as_string())
    server.quit()


def chunks(seq, num):
    """
    Splits a sequence of entities into #num partitions and returns. In case len(seq) % num = k and k != 0, k elements
    will be further distributed. In this case few partitions will have more elements than other.
    :param seq: The sequence to be partitioned
    :param num: Number of partitions to be generated
    :return: list of list of elements
    """
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out


def json_file_to_csv(json_path, csv_path):
    with open(json_path) as handle:
        json_array = json.load(handle)
    json_arr_to_csv(json_array, csv_path)


def json_arr_to_csv(json_array, csv_path):
    # making exhaustive list of column names
    column_names = set()
    for j_element in json_array:
        j_element.pop('description', 'none')
        for key in j_element:
            column_names.add(key)
    column_names = list(column_names)

    f = open(csv_path, 'w')
    f.write("^".join(column_names) + "\n")
    for j_element in json_array:
        vals = [str(j_element.get(key, "null")) for key in column_names]
        line = "^".join(vals) + "\n"
        f.write(line)
    f.flush()
    f.close()


def clean_domains(path):
    lines = ['www.' + line.lower().replace("'", '').replace('"', '').strip() + "\n" for line in open(path)]
    f = open(path, 'w')
    for line in lines:
        f.write(line)
    f.flush()
    f.close()


def csv_to_json(rows, keys):
    j_arr = []
    for entry in rows:
        j_elem = {}
        for index in range(len(entry)):  # iterate over each column of the row
            if entry[index]:  # verifying if the entry is not None or empty strings
                j_elem[keys[index]] = entry[index]
        j_arr.append(j_elem)
    return j_arr


def process_concurrently(input_argument_list: list, processor, post_processor=lambda x: x, batch_size=100,
                         worker_pool_size=5):
    """
    Processes input_argument_list in a concurrent fashion via processor and post_processor functions
    The function splits input_argument_list in multiple batches and calls processor on each batch, collects the output
    of batch and calls post_processor on the list of result of each batch

    input_argument_list -> split into multiple batches with size of batch = batch_size
    for element in batch -> processor(element) -> collect all result (call it results) -> post_processor(results)

    :param input_argument_list: a list of argument which will be processed by processor function
    :param processor: function(object -> object) takes an element of input_argument_list and returns some result. This
    function is called in multithreaded way
    :param post_processor: function(list -> list) takes a list of result and does some processing.
    :param batch_size: max size of a batch of elements for processing
    :param worker_pool_size: Size of the threadpool to spawn
    :return:
    """
    no_of_chunks = len(input_argument_list) / batch_size
    file_parts = chunks(input_argument_list, no_of_chunks)
    post_processor_result_list = []
    processor_result_list = []
    for i in range(len(file_parts)):
        with concurrent.futures.ProcessPoolExecutor(max_workers=worker_pool_size) as executor:
            for entry in executor.map(processor, file_parts[i]):
                processor_result_list.append(entry)
        post_processor_result_list.extend(post_processor(processor_result_list))
        processor_result_list.clear()
    return post_processor_result_list


def getCurrentTimeStamp():
    """
    :return: Timestamp in seconds
    """
    return timegm(datetime.now().utctimetuple())



def map_with_percentage_progress(input_list, func, output_container=None, percentages=tuple(range(0, 100, 5))):
    total_count = len(input_list)
    progress_count_dict = dict(list(zip((map(lambda percentage: int(percentage * total_count / 100), percentages)),
                                        percentages)))
    if output_container:
        result = output_container
    else:
        result = []

    for index in range(total_count):
        result.append(func(input_list[index]))
        if index in progress_count_dict.keys():
            print("completed {} percent job".format(progress_count_dict[index]))
    return result


def read_and_clean_lines(file_path) -> iter:
    return map(lambda line: line.strip().replace('"', '').split(","), open(file_path).readlines())


def file_to_dict_list(file_path) -> list:
    lines = list(read_and_clean_lines(file_path))
    header = lines[0]
    rows = lines[1:]

    def row_to_dict(row):
        d = {}
        for index in range(len(header)):
            d[header[index]] = row[index]
        return d

    return list(map(row_to_dict, rows))


def get_jarr_to_dict(j_arr: list, dict_key_name: str, dict_value_name: str) -> dict:
    mapping = {}
    for entry in j_arr:
        mapping[entry[dict_key_name]] = entry[dict_value_name]
    return mapping


def get_all_values_for_key(j_arr: list, key_name: str) -> list:
    return list(map(lambda entry: entry[key_name], j_arr))


class EventThrottler(object):
    """
    Throttles the event
    The implementation is not thread safe
    """
    def __init__(self, window_length_minutes: int, max_event_count_per_window: int):
        super().__init__()
        self.window_serial_number = 0
        self.window_len = window_length_minutes
        self.max_event_count_per_window = max_event_count_per_window
        self.current_window_event_count = 0
        self.clock_start_time = getCurrentTimeStamp()

    def incrementEventCount(self, count: int):
        self.current_window_event_count = count + self.current_window_event_count

    def __currentWindowEventLimitReached(self):
        self.__resetWindowIfRequired()
        logging.info("current_window_event_count: {} and max_event_count_per_window: {}"
                     .format(self.current_window_event_count, self.max_event_count_per_window))
        return self.current_window_event_count >= self.max_event_count_per_window

    def pauseIfLimitHit(self, sleep_seconds=30):
        if self.__currentWindowEventLimitReached():
            logging.info("eventThrottler putting thread to sleep")
            sleep(sleep_seconds)
            self.pauseIfLimitHit()

    def __resetWindowIfRequired(self):
        calculated_window_serial_number = int((getCurrentTimeStamp() - self.clock_start_time) / (self.window_len * 60))
        if calculated_window_serial_number > self.window_serial_number:
            logging.info("resetting window: calculated_window_serial_number: {} and window_serial_number: {}"
                         .format(calculated_window_serial_number, self.window_serial_number))
            self.window_serial_number = calculated_window_serial_number
            self.current_window_event_count = 0
