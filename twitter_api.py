import tweepy
import time

class TwitterAPI(object):
    def __init__(self, keys, endpoint):
        self.keys = keys
        self.current_key = 0
        self.api = None
        self.rate_id = 0
        self.endpoint = endpoint

        self._find_first_key()

    def _authenticate_key(self, key):
        auth = tweepy.auth.OAuthHandler(key[0], key[1])
        auth.set_access_token(key[2], key[3])
        api = tweepy.API(auth, retry_count=10, retry_delay=5, retry_errors=set([503]))
        self.api = api

    def _change_key(self):
        self.current_key = (self.current_key + 1) % len(self.keys)
        self._authenticate_key(key=self.keys[self.current_key])
        print("Switched to key {}.".format(self.current_key))

    def _find_first_key(self):
        key_found = False

        for i in range(len(self.keys)):
            self._change_key()
            self.update_api_rate()

            if self.rate_id <= 1:
                continue
            else:
                key_found = True
                break

        if not key_found:
            print("All rate ids exhausted. Sleeping for rate reset.")
            self.current_key = 0
            time.sleep(905)

            self._authenticate_key(key=self.keys[self.current_key])
            self.update_api_rate()

    def update_api_rate(self):
        rate_limit_status = self.api.rate_limit_status()

        if self.endpoint == "user_timeline":
            self.rate_id = rate_limit_status['resources']['statuses']['/statuses/user_timeline']['remaining'] - 1
        elif self.endpoint == "followers":
            self.rate_id = rate_limit_status['resources']['followers']['/followers/list']['remaining'] - 1

    def check_rate_id(self, time_start):
        if self.rate_id <= 1:
            self._change_key()
            self.update_api_rate()

            if self.rate_id <= 1:
                time_difference = time.time() - time_start
                if time_difference > 0:
                    print("All rate ids exhausted, sleeping for rate reset.") 
                    time.sleep(905 - time_difference)
                    time_start = time.time()
        return time_start


