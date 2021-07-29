from Credentials import *
import tweepy
import datetime
import pandas as pd


def create_API ():
    authenticator = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    authenticator.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
    api = tweepy.API(authenticator, wait_on_rate_limit=True)
    return api


def execute_query(method, start_time, end_time, query):
    query2 = query + " since:" + str(start_time) + " until:" + str(end_time)
    tweet_list = tweepy.Cursor(method, q=query2, tweet_mode='extended', place_country='BR').items()
    return tweet_list


def organize_query(query):
    output_data = []
    for tweet in query:
        text = tweet._json["full_text"]
        print(text)
        favourite_count = tweet.favorite_count
        retweet_count = tweet.retweet_count
        created_at = tweet.created_at
        author_id = tweet.author.name
        line = {'text' : text, 'favourite_count' : favourite_count, 'retweet_count' : retweet_count, 'created_at' : created_at, 'author_id' : author_id}
        output_data.append(line)
    return output_data


def save_into_db(data):
    dataset = pd.DataFrame(data)
    dataset.to_csv('TwitterScrapper/data.csv')


def main():
    today = datetime.date.today()
    yesterday= today - datetime.timedelta(days=1)
    api = create_API()
    query="#Bolsonaro"
    #query = tweepy.Cursor(api.search, q="#Bolsonaro since:" + str(yesterday)+ " until:" + str(today),tweet_mode='extended', place_country = 'BR').items()
    tweet_list = execute_query(api.search, yesterday, today, query)
    data = organize_query(tweet_list)
    save_into_db(data)


if __name__ == '__main__':
    main()

