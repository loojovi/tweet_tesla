import time
import asyncio

import tweepy
import pandas as pd

from twitter_api import TwitterAPI


class Extractor(object):
    def __init__(self, keys, endpoint):
        self.api = TwitterAPI(keys=keys, endpoint=endpoint)

    def verify_twitter_accounts(self, accounts):
        print('Beginning verification of twitter accounts')
        error=False
        error_list=[]

        for account in accounts:
            try:
                self.api.api.get_user(account)
            except Exception as e:
                print(e)
                error_list.append(account)
                error = True

        if error:
            print("Error: The following users are private/do not exist: {}".format(error_list))
            accounts = [account for account in accounts if account not in error_list]
        return accounts

class TweetExtractor(Extractor):
    def __init__(self, keys):
        super().__init__(keys=keys, endpoint="user_timeline")

    def _process_tweets(self, tweets):
        df_tweets = pd.DataFrame(tweets,
                                 columns=["account", "tweet_id", "tweet", "created_at", "favorite_count",
                                          "retweet_count", "reply_to_user", "reply_to_tweet_id"])

        return df_tweets

    def get_tweets(self, accounts):
        accounts = self.verify_twitter_accounts(accounts=accounts)
        time_start = time.time()

        all_tweets = []
        for account in accounts:
            print("Getting tweets from '{}'.".format(account))
            tweet_list = []
            completed = False
            max_id = None

            while not completed:
                self.api.update_api_rate()
                time_start = self.api.check_rate_id(time_start=time_start)
                new_tweets = self.api.api.user_timeline(screen_name=account, count=200, max_id=max_id)

                if len(new_tweets) > 0:
                    tweet_list.extend(new_tweets)
                    max_id = tweet_list[-1].id - 1
                else:
                    completed = True

            print("Successfully retrieved {} tweets from '{}'.".format(len(tweet_list), account))
            tweets = [[account, tweet.id, tweet.text, tweet.created_at, tweet.favorite_count, tweet.retweet_count,
                       tweet.in_reply_to_screen_name, tweet.in_reply_to_status_id] for tweet in tweet_list]
            all_tweets.extend(tweets)

        df_tweets = self._process_tweets(tweets=all_tweets)

        return df_tweets

class FollowerIDExtractor(Extractor):
    def __init__(self, keys):
        super().__init__(keys=keys, endpoint="follower_ids")
        self.max_count = 5000

    def _process_follower_ids(self, follower_ids):
        df_follower_ids = pd.DataFrame(follower_ids, columns=["account", "follower_user_id"])
        return df_follower_ids

    def get_follower_ids(self, accounts):
        accounts = self.verify_twitter_accounts(accounts=accounts)
        time_start = time.time()

        all_followers = []
        for account in accounts:
            print("Getting follower ids of '{}'.".format(account))
            follower_list = []
            completed = False
            cursor = -1

            pages = tweepy.Cursor(self.api.api.followers_ids, screen_name=account, count=self.max_count).pages()

            while not completed:
                try:
                    new_users = pages.next()
                    follower_list.extend(new_users)

                    if len(new_users) < self.max_count:
                        completed = True
                except Exception as e:
                    self.api.update_api_rate()
                    time_start = self.api.check_rate_id(time_start=time_start)
                    follower_list.extend(new_users)
                    pages = tweepy.Cursor(self.api.api.followers_ids,
                                          screen_name=account,
                                          count=self.max_count,
                                          cursor=pages.next_cursor).pages()
            
            print("Succesfully retrived {} followers from '{}'.".format(len(follower_list), account))

            followers = [[account, user_id] for user_id in follower_list]
            all_followers.extend(followers)

        df_follower_ids = self._process_follower_ids(follower_ids=all_followers)

        return df_follower_ids

class UserExtractor(Extractor):
    def __init__(self, keys):
        super().__init__(keys=keys, endpoint="users")
        self.max_count = 100-1

    def _process_users(self, users):
        df_users = pd.DataFrame(users,
                                columns=["user_id", "user_name", "screen_name", "location",
                                         "description", "follower_count", "following_count", "listed_count",
                                         "favorite_count", "status_count", "created_at", "profile_image_url",
                                         "default_profile_image"])

        return df_users

    async def get_users(self, follower_ids):
        time_start = time.time()
        new_users = None

        try:
            new_users = self.api.api.lookup_users(user_ids=follower_ids)
        except tweepy.TweepError as e:
            print(e)
            self.api.update_api_rate()
            time_start = self.api.check_rate_id(time_start=time_start)
            new_users = self.api.api.lookup_users(user_ids=follower_ids)
        except Exception as e:
            print(e)

        if new_users is not None:
            new_users = [[user.id, user.name, user.screen_name, user.location, user.description,
                          user.followers_count, user.friends_count, user.listed_count, user.favourites_count,
                          user.statuses_count, user.created_at, user.profile_image_url_https, 
                          user.default_profile_image] for user in new_users]
            df_users = self._process_users(users=new_users)

        return df_users



def combine_results(result):
    global results
    results.append(result)


def split_df(df_follower_ids, batch_size):
    lst = []
    for i in range(0, df_follower_ids.shape[0]-batch_size, batch_size):
        follower_ids = df_follower_ids.loc[i:i+batch_size, "follower_user_id"].tolist()
        lst.append(follower_ids)

    return lst

async def get_all_users(user_extractor: object, follower_lst: list) -> list:
    return await asyncio.gather(*(user_extractor.get_users(followers) for followers in follower_lst))

if __name__=="__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--get_tweets",
                        required=False,
                        type=str,
                        default="False")
    parser.add_argument("--get_users",
                        required=False,
                        type=str,
                        default="False")
    parser.add_argument("--get_follower_ids",
                        required=False,
                        type=str,
                        default="False")
    args = parser.parse_args()

    get_tweets = args.get_tweets.lower()
    get_tweets = get_tweets == "true"

    get_users = args.get_users.lower()
    get_users = get_users == "true"

    get_follower_ids = args.get_follower_ids.lower()
    get_follower_ids = get_follower_ids == "true"

    MAIN_FOLDER = os.path.dirname(__file__)
    ACCESS_TOKEN_FILE = os.path.join(MAIN_FOLDER, "accesstoken.csv")

    TWEET_FOLDER = os.path.join(MAIN_FOLDER, "data", "tweets")
    USER_FOLDER = os.path.join(MAIN_FOLDER, "data", "users")
    FOLLOWER_ID_FOLDER = os.path.join(MAIN_FOLDER, "data", "follower_ids")

    TWEET_FILE = os.path.join(TWEET_FOLDER, "tweets.csv")
    FOLLOWER_ID_FILE = os.path.join(FOLLOWER_ID_FOLDER, "follower_ids.csv")
    USER_FILE = os.path.join(USER_FOLDER, "users.csv")

    accounts = ['kia', 'hyundai', 'VW', 'tesla']
    #accounts = ["Zo2420"]

    if not os.path.isdir(TWEET_FOLDER):
        os.mkdir(TWEET_FOLDER)

    if not os.path.isdir(USER_FOLDER):
        os.mkdir(USER_FOLDER)

    if not os.path.isdir(FOLLOWER_ID_FOLDER):
        os.mkdir(FOLLOWER_ID_FOLDER)

    access_tokens = pd.read_csv(ACCESS_TOKEN_FILE)
    access_tokens = access_tokens.values.tolist()

    if get_tweets:
        tweet_extractor = TweetExtractor(keys=access_tokens)
        df_tweets = tweet_extractor.get_tweets(accounts=accounts)
        df_tweets.to_csv(TWEET_FILE, index=False)

    if get_follower_ids:
        follower_id_extractor = FollowerIDExtractor(keys=access_tokens)
        df_follower_ids = follower_id_extractor.get_follower_ids(accounts=accounts)
        df_follower_ids.to_csv(FOLLOWER_ID_FILE, index=False)
    
    if get_users:
        user_extractor = UserExtractor(keys=access_tokens)

        if os.path.isfile(FOLLOWER_ID_FILE):
            df_follower_ids = pd.read_csv(FOLLOWER_ID_FILE)
            df_follower_ids_formatted = split_df(df_follower_ids=df_follower_ids, batch_size=99)

            df_users = asyncio.run(get_all_users(user_extractor=user_extractor, follower_lst=df_follower_ids_formatted))

            df_users = pd.concat(df_users)
            df_users.to_csv(USER_FILE, index=False)


