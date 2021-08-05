from Credentials import *
import tweepy
import datetime
import pandas as pd
import psycopg2


##### Function to estabilish connection to twiter API
def create_API ():
    authenticator = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    authenticator.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
    api = tweepy.API(authenticator, wait_on_rate_limit=True)
    return api


#### Function to execute a query into the twitter API, the method is refered to the tweepy query methods such as search,  user_timeline etc
def execute_query(method, start_time, end_time, query=""):
    query2 = query + " since:" + str(start_time) + " until:" + str(end_time)
    print("Exectuting query")
    tweet_list = tweepy.Cursor(method, q=query2, tweet_mode='extended', place_country='BR').items()
    print("Query complete")
    return tweet_list


#### Function to organize the data extracted from the twitter API, the raw data is a JSON with a lot of information, this function selects only
#### the necessary data for the future use
def organize_query(query):
    output_data = []
    for tweet in query:
        line = {'id': tweet.id,
                'text' : tweet.full_text,
                'favorite_count' : tweet.favorite_count,
                'retweet_count' : tweet.retweet_count,
                'created_at' : tweet.created_at, 
                'author_id' : tweet.author.name}
        output_data.append(line)
    return output_data


#### Function used to save the twitter data into a csv file, the data is added cumulatively
def update_into_csv(data, path):
    print("\nUpdating data into csv")
    dataset = pd.DataFrame(data)
    dataset.set_index("id", inplace=True)
    dataset.to_csv(path, mode='a', header=False)
    print("\nUpdate Complete")


#### Function to connect to the PostgreSQL database and save the data scrapped into a table of this database
def save_into_db():
    print("\nConnecting to database")
    conn = psycopg2.connect(host=SQL_HOST, database=SQL_DATABASE,
    user=SQL_USER, password=SQL_PASSWORD)
    print("\nConnection estabilished")
    cur = conn.cursor()
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    table='twitter_data'
    print("\nExecuting Query")
    with open('TwitterScrapper/data.csv', 'r', encoding='utf8') as file:
        cur.execute("truncate " + table + ";")
        cur.copy_expert(sql=sql % table, file=file)
    conn.commit()
    print("\nQuery finished")


#### main function
def main():
    today = datetime.date.today()
    yesterday= today - datetime.timedelta(days=2)
    day_before = today - datetime.timedelta(days=5)
    api = create_API()
    query="from:jairbolsonaro"
    path = 'TwitterScrapper/data.csv'
    tweet_list = execute_query(api.search, day_before, yesterday, query)
    data = organize_query(tweet_list)
    update_into_csv(data, path)
    save_into_db()



if __name__ == '__main__':
    main()

