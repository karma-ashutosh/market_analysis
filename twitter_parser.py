import json
from calendar import timegm
from email._parseaddr import parsedate
from threading import Thread
from time import sleep

import logging
import yaml
import twitter
from postgres_io import PostgresIO
from datetime import datetime, timedelta

from tweet_processor import process_new_tweets

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
with open('./config.yml') as handle:
    config = yaml.load(handle)
feed_table = config['postgres-config']['twitter.feed.table']
companies_to_track_table = config['postgres-config']['comany_list_table']
postgres = PostgresIO(config['postgres-config'])
postgres.connect()
api_retry_sleep_seconds = 30
api = twitter.Api(consumer_key='1bPapMyYjQ2jjJSpsAEzBE3Ej',
                  consumer_secret='osCMeCZzHo3eFoTPD2qFUWSuDNOpcgTB7ll7Iie682t99ekh9A',
                  access_token_key='2435429772-5X556hGhJ4hPGCk8xmeqkrDM4iFbuvH8Wu5enlV',
                  access_token_secret='0gwr6XNcvB9PViGcYo43iGJ1JofwFMG759owapyeOQMGH', tweet_mode='extended')


class TimeLineRequest(Thread):
    def __init__(self):
        super().__init__()
        self.user_id = None
        self.screen_name = None
        self.since_id = None
        self.max_id = None
        self.count = None
        self.include_rts = True
        self.trim_user = False
        self.exclude_replies = False

    def run(self):
        return api.GetUserTimeline(user_id=self.user_id, screen_name=self.screen_name, since_id=self.since_id,
                                   max_id=self.max_id, count=self.count, include_rts=self.include_rts,
                                   trim_user=self.trim_user, exclude_replies=self.exclude_replies)

    def __str__(self) -> str:
        return json.dumps(
            {
                'user_id': self.user_id,
                'screen_name': self.screen_name,
                'since_id': self.since_id,
                'max_id': self.max_id,
                'count': self.count,
                'include_rts': self.include_rts,
                'trim_user': self.trim_user,
                'exclude_replies': self.exclude_replies
            }
        )


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
        self.current_window_event_count += count

    def __currentWindowEventLimitReached(self):
        self.__resetWindowIfRequired()
        return self.current_window_event_count >= self.max_event_count_per_window

    def pauseIfLimitHit(self):
        if self.__currentWindowEventLimitReached():
            sleep(30)
            self.pauseIfLimitHit()

    def __resetWindowIfRequired(self):
        calculated_window_serial_number = (getCurrentTimeStamp() - self.clock_start_time) / (self.window_len * 60)
        if calculated_window_serial_number > self.window_serial_number:
            self.window_serial_number = calculated_window_serial_number
            self.current_window_event_count = 0


def fetchAndPersistTillEnd(twitter_handle, maxDepth=1000, maxDays=90):
    """
    The method should be called for the very first time (when we are starting to populate tweet data). It starts reading
    from the most recent tweet till one of the limits (maxDepth or maxDays) is met
    :param twitter_handle:
    :param maxDepth:
    :param maxDays:
    :return:
    """
    request_obj = TimeLineRequest()
    request_obj.screen_name = twitter_handle
    request_obj.count = 200
    request_obj.exclude_replies = False
    timeline_entries = list(map(lambda status: status.AsDict(),
                                execute_with_retry(request_obj, sleep_interval_seconds=api_retry_sleep_seconds)))
    depth = 0
    epoch_lower_time_limit = timegm((datetime.now() - timedelta(maxDays)).utctimetuple())
    tweet_time_criteria_met = False
    while depth < maxDepth:
        if tweet_time_criteria_met or len(timeline_entries) == 0:
            break
        tweet_time_criteria_met, tweet_list = parse_tweet_list(timeline_entries,
                                                               epoch_lower_time_limit=epoch_lower_time_limit)
        depth += len(tweet_list)
        flush(tweet_list)
        request_obj.max_id = min([int(tweet['user_status_id']) for tweet in tweet_list]) - 1
        timeline_entries = list(map(lambda status: status.AsDict(),
                                    execute_with_retry(request_obj, sleep_interval_seconds=api_retry_sleep_seconds)))
    return depth


def parse_tweet_list(timeline_entries, epoch_lower_time_limit=0):
    tweet_list = []
    tweet_time_criteria_met = False
    for entry in timeline_entries:
        source, tweet_type = (entry['quoted_status'], 'quoted_status') if 'quoted_status' in entry.keys() \
            else (entry['retweeted_status'], 'retweeted_status') if 'retweeted_status' in entry.keys() else \
            ({}, 'original_status')
        source_text, source_time, source_handle, source_status_id = source.get('full_text'), source.get('created_at'), \
                                                                    source.get('user', {}).get('screen_name'), \
                                                                    source.get('id_str')
        user_text, user_time, user_handle, user_status_id = entry['full_text'] if entry != 'retweeted_status' else \
                                                                None, entry['created_at'], entry['user']['screen_name'], \
                                                            entry['id_str']
        result = {
            'source_text': source_text, 'source_time': source_time,
            'source_twitter_handle': lower(source_handle), 'source_status_id': source_status_id,
            'user_text': user_text, 'user_time': user_time,
            'user_twitter_handle': lower(user_handle), 'user_status_id': user_status_id
        }
        tweet_epoch = timegm(parsedate(user_time))
        tweet_time_criteria_met = tweet_epoch < epoch_lower_time_limit
        if tweet_time_criteria_met:
            break
        tweet_list.append(result)
    return tweet_time_criteria_met, tweet_list


def fetchSinceLatestReadTweet(twitter_handle):

    fetch_size = 200
    request_obj = TimeLineRequest()
    request_obj.exclude_replies = False
    request_obj.count = fetch_size
    request_obj.screen_name = twitter_handle
    request_obj.since_id = latestTweetIdForUser(twitter_handle)
    timeline_entries = list(map(lambda status: status.AsDict(),
                                execute_with_retry(request_obj, sleep_interval_seconds=api_retry_sleep_seconds)))
    api_request_count = 1
    _, tweet_list = parse_tweet_list(timeline_entries)
    flush(tweet_list)
    if len(tweet_list) >= fetch_size:
        api_request_count += fetchSinceLatestReadTweet(twitter_handle)
    return api_request_count


def flush(j_arr: list):
    postgres.insert_jarr(j_arr, feed_table)


def latestTweetIdForUser(twitter_handle):
    query = "SELECT MAX(user_status_id) FROM {} WHERE user_twitter_handle = '{}'".format(feed_table, twitter_handle)
    return postgres.execute([query], fetch_result=True)['result'][0]['max']


def execute_with_retry(obj: Thread, retry_count=5, sleep_interval_seconds=1):
    exception = None
    current_count = 0
    while current_count < retry_count:
        try:
            return obj.run()
        except Exception as e:
            logging.info("{}th try failed with obj {}".format(current_count, obj.__str__()))
            current_count += 1
            exception = e
            sleep(sleep_interval_seconds)
    raise exception


def lower(s):
    return str(s).lower()


def getCurrentTimeStamp():
    """
    :return: Timestamp in seconds
    """
    return timegm(datetime.now().utctimetuple())


if __name__ == '__main__':
    while True:
        request_count = 0
        eventThrottler = EventThrottler(window_length_minutes=15, max_event_count_per_window=1000)
        startTime = getCurrentTimeStamp()
        # .rsplit("/", 1)[1]
        twitter_handles = list(map(lambda j: j['handle'],
                                   postgres.execute(['SELECT handle FROM {} WHERE parse_flag = true'
                                                    .format(companies_to_track_table)], fetch_result=True)['result']))
        for twitter_handle in twitter_handles:
            eventThrottler.pauseIfLimitHit()
            try:
                logging.info("working on {}".format(twitter_handle))
                latestTweetId = latestTweetIdForUser(twitter_handle)
                if latestTweetId:
                    request_count = fetchSinceLatestReadTweet(twitter_handle)
                else:
                    request_count = fetchAndPersistTillEnd(twitter_handle)
                logging.info("made {} api calls".format(request_count))
            except:
                logging.exception("exception with handle: {}".format(twitter_handle))
            eventThrottler.incrementEventCount(request_count)

        process_new_tweets()
        timeElapsed = getCurrentTimeStamp() - startTime
        sleep_time = 5 * 60 - timeElapsed
        sleep_time = 0 if sleep_time < 0 else sleep_time
        logging.info("processing done for the day. Sleeping for {} seconds".format(sleep_time))
        sleep(sleep_time)
