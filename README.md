# Brief Description

TO BE UPDATED

# How to Install

1. Clone repository.

```bash
git clone https://github.com/loojovi/tweet_tesla.git
```

2. Install required packages.

```bash
pip install -r requirements.txt
```

3. Add accesstokens.csv containing twitter API keys to ```/tweet_tesla```.

   Columns:
      * ConsumerKey
      * ConsumerSecret
      * AccessKey
      * AccessSecret

# How to Run

1. To extract data from twitter.

```bash
python twitter_extractors.py --help

usage: twitter_extractors.py [-h] [--get_tweets GET_TWEETS]
                             [--get_users GET_USERS]
                             [--get_followers GET_FOLLOWERS]

optional arguments:
  -h, --help            show this help message and exit
  --get_tweets GET_TWEETS
  --get_users GET_USERS
  --get_followers GET_FOLLOWERS
```

