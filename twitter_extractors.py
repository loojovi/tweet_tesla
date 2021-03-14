import time

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

class FollowerExtractor(Extractor):
    def __init__(self, keys):
        super().__init__(keys=keys, endpoint="followers")
        self.max_count = 5000

    def _process_followers(self, followers):
        """
        df_followers = pd.DataFrame(followers,
                                    columns=["account", "user_id", "user_name", "screen_name", "location",
                                             "description", "follower_count", "following_count", "listed_count",
                                             "favorite_count", "status_count", "created_at", "profile_image_url",
                                             "default_profile_image"])
        """

        df_followers = pd.DataFrame(followers, columns=["account", "follower_user_id"])
        return df_followers

    def get_followers(self, accounts):
        accounts = self.verify_twitter_accounts(accounts=accounts)
        time_start = time.time()

        all_followers = []
        for account in accounts:
            print("Getting followers of '{}'.".format(account))
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
                    print(e)
                    self.api.update_api_rate()
                    time_start = self.api.check_rate_id(time_start=time_start)
                    follower_list.extend(new_users)
                    pages = tweepy.Cursor(self.api.api.followers_ids,
                                          screen_name=account,
                                          count=self.max_count,
                                          cursor=pages.next_cursor).pages()
            
            print("Succesfully retrived {} followers from '{}'.".format(len(follower_list), account))
            """
            followers = [[account, user.id, user.name, user.screen_name, user.location, user.description,
                          user.followers_count, user.friends_count, user.listed_count, user.favourites_count,
                          user.statuses_count, user.created_at, user.profile_image_url_https, 
                          user.default_profile_image] for user in follower_list]
            """

            followers = [[account, user_id] for user_id in follower_list]
            all_followers.extend(followers)

        df_followers = self._process_followers(followers=all_followers)

        return df_followers





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
    parser.add_argument("--get_followers",
                        required=False,
                        type=str,
                        default="False")
    args = parser.parse_args()

    get_tweets = args.get_tweets.lower()
    get_tweets = get_tweets == "true"

    get_users = args.get_users.lower()
    get_users = get_users == "true"

    get_followers = args.get_followers.lower()
    get_followers = get_followers == "true"

    MAIN_FOLDER = os.path.dirname(__file__)
    ACCESS_TOKEN_FILE = os.path.join(MAIN_FOLDER, "accesstoken.csv")

    TWEET_FOLDER = os.path.join(MAIN_FOLDER, "data", "tweets")
    USER_FOLDER = os.path.join(MAIN_FOLDER, "data", "users")
    FOLLOWER_FOLDER = os.path.join(MAIN_FOLDER, "data", "followers")

    accounts = ['kia', 'hyundai', 'VW']#, 'tesla']
    #accounts = ["Zo2420"]

    if not os.path.isdir(TWEET_FOLDER):
        os.mkdir(TWEET_FOLDER)

    if not os.path.isdir(USER_FOLDER):
        os.mkdir(USER_FOLDER)

    if not os.path.isdir(FOLLOWER_FOLDER):
        os.mkdir(FOLLOWER_FOLDER)

    access_tokens = pd.read_csv(ACCESS_TOKEN_FILE)
    access_tokens = access_tokens.values.tolist()

    if get_tweets:
        tweet_extractor = TweetExtractor(keys=access_tokens)
        df_tweets = tweet_extractor.get_tweets(accounts=accounts)
        df_tweets.to_csv(os.path.join(TWEET_FOLDER, "tweets.csv"), index=False)

    if get_followers:
        follower_extractor = FollowerExtractor(keys=access_tokens)
        df_followers = follower_extractor.get_followers(accounts=accounts)
        df_followers.to_csv(os.path.join(FOLLOWER_FOLDER, "followers.csv"), index=False)
    
