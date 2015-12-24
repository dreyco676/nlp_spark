import json
from datetime import datetime
from datetime import timedelta
from time import sleep
from time import time

import pandas as pd
from twython import Twython
from twython import exceptions as twe

from aquire_data import secrets


class TwitterClient(object):
    REQUEST_LATENCY = 0.2

    def __init__(self):
        self.next_req_time = datetime.fromtimestamp(0)
        self.rate_window = 900

    def _app_auth(self):
        # twitter user level credentials come from python file
        app_key = secrets.twitter_app_secret()
        access_token = secrets.twitter_access_token()
        self.twauth = Twython(app_key, access_token=access_token, oauth_version=2)
        return

    def get_followers(self, handle):
        result_df = pd.DataFrame()
        self._app_auth()
        cursor = -1
        while cursor != 0:
            data = self._follower_req(handle, cursor)
            if len(data) != 0:
                cursor = data['next_cursor']
                partial_df = pd.DataFrame(data['ids'])
                partial_df = partial_df.rename(columns={0: 'User_Id'})
                # append to dataframe
                result_df = result_df.append(partial_df)
            else:
                cursor = 0
        return result_df

    def _follower_req(self, twid, cursor):
        data = []
        try:
            self._wait_for_rate_limit()
            data = self.twauth.get_followers_ids(screen_name=twid, cursor=cursor, count=5000, skip_status=True)
            self._update_rate_limit()
        except (twe.TwythonRateLimitError, TimeoutError) as e:
            print(e)
            sleep(self.rate_window)
            self._app_auth()
            data = self._follower_req(twid, cursor)
        except twe.TwythonAuthError as e:
            print(e)
        except twe.TwythonError as e:
            print(e)
        return data

    def get_timeline(self, twid, filepath):
        self._app_auth()
        max_id = None
        # only can pull 3200 tweets per timeline
        max_timeline = 3200
        tweet_cnt = 0
        while tweet_cnt < max_timeline:
            data = self._timeline_req(str(twid), max_id)
            tweets_in_resp = len(data)
            if tweets_in_resp != 0:
                tweet_cnt += tweets_in_resp
                prev_max_id = max_id
                max_id = data[-1]['id']
                # if the id is the same as the previous, you are on the earliest page
                if max_id == prev_max_id:
                    break
                else:
                    self._parse_timeline(data, filepath)
            # no more data to return
            else:
                break
        return

    def _timeline_req(self, twid, max_id):
        data = []
        try:
            self._wait_for_rate_limit()
            data = self.twauth.get_user_timeline(user_id=twid, count=200, exclude_replies=True, max_id=max_id)
            self._update_rate_limit()
        except (twe.TwythonRateLimitError, TimeoutError) as e:
            print(e)
            sleep(self.rate_window)
            self._app_auth()
            data = self._timeline_req(twid, max_id)
        except twe.TwythonAuthError as e:
            print(e)
        except twe.TwythonError as e:
            print(e)
        return data

    def _parse_timeline(self, json_data, filepath):
        with open(filepath, "a+") as outfile:
            for tweetObject in json_data:
                json.dump(tweetObject, outfile)
                outfile.write('\n')
        return

    def _wait_for_rate_limit(self):
        now = datetime.now()
        if self.next_req_time > now:
            t = self.next_req_time - now
            sleep(t.total_seconds())

    def _update_rate_limit(self):
        try:
            remaining = float(self.twauth.get_lastfunction_header('X-Rate-Limit-Remaining'))
            # time in seconds since epoch that it resets
            reset_time = float(self.twauth.get_lastfunction_header('X-Rate-Limit-Reset'))
        except TypeError:
            remaining = 0
            reset_time = 900
        reset = reset_time - float(time())
        spacing = reset / (1.0 + remaining)
        delay = spacing + self.REQUEST_LATENCY
        self.next_req_time = datetime.now() + timedelta(seconds=delay)
