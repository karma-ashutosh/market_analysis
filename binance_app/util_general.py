import logging
import smtplib
from calendar import timegm
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from os.path import basename
from util_linux import create_dir_if_not_exists

timed_log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
msg_only_formatter = logging.Formatter('%(message)s')


def setup_logger(name, log_file, level=logging.INFO, logging_format=True, msg_only=False, log_rotation_unit='h',
                 log_rotation_interval=4):
    from logging import handlers
    """Function setup as many loggers as you want"""
    create_dir_if_not_exists(log_file)
    # handler = logging.FileHandler(log_file)
    handler = handlers.TimedRotatingFileHandler(log_file, when=log_rotation_unit, interval=log_rotation_interval)
    if logging_format:
        if msg_only:
            handler.setFormatter(msg_only_formatter)
        else:
            handler.setFormatter(timed_log_formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


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


epoch = datetime.utcfromtimestamp(0)


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def getCurrentTimeStamp():
    """
    :return: Timestamp in seconds
    """
    return timegm(datetime.now().utctimetuple())


def strptime(val) -> datetime:
    if '.' not in val:
        return datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")

    nofrag, frag = val.split(".")
    date = datetime.strptime(nofrag, "%Y-%m-%dT%H:%M:%S")

    frag = frag[:6]  # truncate to microseconds
    frag += (6 - len(frag)) * '0'  # add 0s
    return date.replace(microsecond=int(frag))


app_logger = setup_logger('app_logger', '../logs/app.log', msg_only=True)
