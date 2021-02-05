# This code returns:
# 1. Tweets of target user in the past 7 days. Output is in tweet_df.
# 2. Target user profile. Output is in user_df.
# 3. Word Frequency Table (after removing stopwords, etc). Output is in word_count
# 4. Tweet sentiment score and sentiment classification. Output is in tweet_sentiment

import requests
import pandas as pd
import sys
import json

# For connection to Amazon Redshift
#pip install psycopg2
#pip install sqlalchemy
import sqlalchemy
from sqlalchemy import create_engine

from textblob import TextBlob

from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType
from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover

# Bearer Token for Twitter API
bearer_token = "AAAAAAAAAAAAAAAAAAAAAE9GIwEAAAAA3QrOBU3z5%2BWYt4M%2FJyHJjcpeRfg%3DdLsH6iXulCBHEjYbD59iMyHNLU9oqDuj0fWceP3lpbLl2lXi9y"

# Connection to AWS RedShift. Schema for the connection engine is "postgresql+psycopg2://<username>:<cluster_url>:<port>/<cluster>"
redshift_conn = create_engine(
    'postgresql+psycopg2://<username>:<Redshift DB URL>:5439/dev')

# Check if the dataframe has a particular column. Used for handling some tweets that has geolocation field.
def has_column(df, col):
    try:
        df[col]
        return True
    except:
        return False

# Function to create the URL request to sent to Twitter to retrieve past 7 days tweets of specified
# username. As each return is only 10 entries, one will need to iterate through the various "pages"
def create_url(target="", next_token=""):
    query = "from:{} -is:retweet".format(target)

    if next_token =="" :
        next_t = ""
    else:
        next_t = "next_token=" + next_token

    # Tweet object fields to include in tweet response
    tweet_fields = "tweet.fields=author_id,id,created_at,geo,text"
    expansions = "expansions=author_id"
    # User object fields to include in the tweet response
    user_fields = "user.fields=created_at,name,verified,username,location,public_metrics"

    # Combining the URL options into the required format
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}".format(
        query, next_t, tweet_fields, expansions, user_fields
    )
    return url

# Function to create the header for Twitter authentication
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

# Function to connect to Twitter
def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)

    # If connection is not successful, raise the exception. Otherwise, return the Twitter response
    # JSON object.
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# Function to take in the sentiment score, and classify into negative, positive or neutral
def classify_sentiment(x):
    if x < 0:
        return "negative"
    elif x > 0:
        return "positive"
    else:
        return "neutral"

# Function to merge user_df and public_metrics dataframe
def with_column_index(sdf):
    new_schema = StructType(sdf.schema.fields + [StructField("ColumnIndex", LongType(), False), ])
    return sdf.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)

# Main function that will run using the Twitter username sent over by receive_spark_stream Python script.
def main(username):
    # For verification on the username received and print in console for demo. For actual deployment,
    # can comment away.
    print(f"Received username= {username}")

    # Start the Spark instance
    cnfg = SparkConf().setAppName("TwitterUserProfile").setMaster("local[2]")
    sc = SparkContext(conf=cnfg)
    spark = SparkSession(sc)

    # Initialise the first page of tweets & user (1 page consist of 10 entries)
    url = create_url(target=username)
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url, headers)

    # Parsing the JSON response returned by Twitter
    tweet_df = spark.createDataFrame(json_response['data'])

    # Check if there's geolocation field in the response.
    geo_exist = has_column(tweet_df,"geo")

    # Extracting the geolocation information via geo.place_id
    if geo_exist:
        tweet_df = tweet_df.select("author_id","created_at","geo.place_id","id","text")
    else:
        tweet_df = tweet_df.select("author_id", "created_at", "id", "text")

    # Extracting the user details
    user_df = spark.createDataFrame(json_response['includes']['users'])

    # flatten the public_metrics
    cols = list(map(
        lambda f: F.col("public_metrics").getItem(f).alias(str(f)),
        ["following_count", "tweet_count", "listed_count", "followers_count"]))

    public_metrics = user_df.select(cols)
    user_df = user_df.drop('public_metrics')

    # Merge user_df with public_metrics
    user_df = with_column_index(user_df)
    public_metrics = with_column_index(public_metrics)
    user_df = user_df.join(public_metrics, user_df.ColumnIndex == public_metrics.ColumnIndex, 'inner').drop("ColumnIndex")

    # If there are more tweets (next page / next token), append it to tweet_df.
    # user_df is just for a single user, so no need to append. Info will be the same.

    if 'next_token' not in json_response['meta']:
        pass
    else:
        next_token = json_response['meta']['next_token']

        while next_token is not None:
            url = create_url(username, next_token)
            json_response = connect_to_endpoint(url, headers)

            new_tweets = spark.createDataFrame(json_response['data'])

            # Check if there's geolocation field in the new tweets
            new_tweet_geo_exist = has_column(new_tweets, "geo")

            if new_tweet_geo_exist:
                new_tweets = new_tweets.select("author_id", "created_at", "geo.place_id", "id", "text")
            else:
                new_tweets = new_tweets.select("author_id", "created_at", "id", "text")

            # to make sure all have the same number of columns
            for column in tweet_df.columns:
                if column not in new_tweets.columns:
                    new_tweets = new_tweets.withColumn(column, F.lit(None))

            for column in new_tweets.columns:
                if column not in tweet_df.columns:
                    tweet_df = tweet_df.withColumn(column, F.lit(None))

            # Reordering the column of new_tweets for union function
            if geo_exist:
                new_tweets = new_tweets.select("author_id","created_at","place_id","id","text")
            else:
                new_tweets = new_tweets.select("author_id", "created_at", "id", "text")

            tweet_df = tweet_df.union(new_tweets)

            if 'next_token' not in json_response['meta']:
                next_token = None
            else:
                next_token = json_response['meta']['next_token']

    # Show the df. Can comment away in actual production.
    tweet_df.show(truncate=False)
    user_df.show(truncate=False)

    # Extract geolocation information within the tweets. Currently not in use.
    if geo_exist:
        location_df = tweet_df.select("author_id","id","place_id").dropna()
        location_df.show(truncate=False)

    # WORD FREQUENCY - to be made into word cloud in Tableau or other visualisation software.
    tweet_only = tweet_df.select("author_id", "text")

    # Remove punctuation, covert to lower case
    df_clean = tweet_only.select("author_id", (lower(regexp_replace('text', "[^a-zA-Z\\s]", "")).alias('text')))

    # Tokenize text
    tokenizer = Tokenizer(inputCol='text', outputCol='words_token')
    df_words_token = tokenizer.transform(df_clean).select('author_id', 'words_token')

    # Remove stop words
    remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
    df_words_no_stopw = remover.transform(df_words_token).select('author_id', 'words_clean')

    # Filter length word > 3
    filter_length_udf = udf(lambda row: [x for x in row if 3 <= len(x) <= 13], ArrayType(StringType()))
    df_final_words = df_words_no_stopw.withColumn('words', filter_length_udf(col('words_clean')))

    # Printing the word list. Can comment away in actual deployment.
    df_final_words.show(truncate=False)

    word_count = df_final_words.select('author_id', F.explode('words').alias('word')).\
        groupBy('author_id', 'word').\
        count().\
        sort('count', ascending=False)

    # Printing the word list and count. Can comment away in actual deployment.
    word_count.show()

    # SENTIMENT ANALYSIS. Sentiment is in the range of (-1, 1).
    sentiment = udf(lambda x: TextBlob(x).sentiment[0])
    tweet_sentiment = tweet_df.withColumn("sentiment_score", sentiment(tweet_df["text"]).cast("double"))

    classify_sentiment_udf = udf(classify_sentiment)

    tweet_sentiment = tweet_sentiment.withColumn("sentiment", classify_sentiment_udf(tweet_sentiment["sentiment_score"]))
    tweet_sentiment = tweet_sentiment.select('author_id', 'created_at', 'id', 'text', 'sentiment_score', 'sentiment')
    # Can comment away the show statement. Left here to display the progress in console for demo.
    tweet_sentiment.show()


    sentiment_count = tweet_sentiment.groupBy('author_id', 'sentiment').agg(F.mean('sentiment_score'), F.count('sentiment')).toDF("author_id", "sentiment", "avg_sentiment_score", "count")
    # Can comment away the show statement. Left here to display the progress in console for demo.
    sentiment_count.show()

    # Read in existing data from Amazon RedShift DB. If user already exists, need to merge and deduplicate, then write data back.
    with redshift_conn.connect() as conn, conn.begin():

        # Check if Table exists first. If so, read in existing Twitter users that are already in RedShift DB.
        # The unique key is the id, which is the author_id, Twitter user id.
        if redshift_conn.has_table("user_data"):
            user = pd.read_sql("""
               select * from user_data;""", conn)

            # Append latest data retrieved to those in DB and remove duplicates, keeping the latest.
            user = user.append(user_df.toPandas())
            user = user.drop_duplicates(subset="id", keep="last")
        else:
            user = user_df.toPandas()

        # Similarly, check if the Table for sentiment count exists. If so, read in existing sentiment count
        # for existing users in RedShift DB. The pair, author_id and sentiment," is used for deduplication.
        if redshift_conn.has_table("sentiment_count"):
            senti_df = pd.read_sql("""
               select * from sentiment_count;""", conn)

            # Append latest data to those in DB and remove duplicates, keeping the latest.
            senti_df = senti_df.append(sentiment_count.toPandas())
            senti_df = senti_df.drop_duplicates(subset=["author_id", "sentiment"], keep="last")
        else:
            senti_df = sentiment_count.toPandas()

        # Checking if Table for word_count already exists in RedShift. If so, read in existing word count for
        # existing users in RedShift DB. Distinct pair of author_id and word is used for comparison.
        if redshift_conn.has_table("word_count"):
            word_df = pd.read_sql("""
                       select * from word_count;""", conn)

            # Append latest data to those in DB and remove duplicates, keeping the latest.
            word_df = word_df.append(word_count.toPandas())
            word_df = word_df.drop_duplicates(subset=["author_id", "word"], keep="last")
        else:
            word_df = word_count.toPandas()

        # Check for Table, tweet_sentiment. If exists, read in existing tweet sentiment for existing users in
        # RedShift DB. The unique ID used is the tweet id, which is unique for each tweet. All unique tweets
        # are kept. Thus even if the Twitter user deleted his old tweets, it will still be retained in the
        # Redshift DB if it was previously captured.
        if redshift_conn.has_table("tweet_sentiment"):
            tweet_db = pd.read_sql("""
                       select * from tweet_sentiment;""", conn)

            # Append latest data to those in DB and remove duplicates, keeping the latest.
            tweet_db = tweet_db.append(tweet_sentiment.toPandas())
            tweet_db = tweet_db.drop_duplicates(subset="id", keep="last")
        else:
            tweet_db = tweet_sentiment.toPandas()

    # Update the data to Redshift.
    user.to_sql('user_data', redshift_conn, index=False, if_exists='replace')
    word_df.to_sql('word_count', redshift_conn, index=False, if_exists='replace')
    senti_df.to_sql('sentiment_count', redshift_conn, index=False, if_exists='replace')
    tweet_db.to_sql('tweet_sentiment', redshift_conn, index=False, if_exists='replace',
                    dtype={
                        'author_id': sqlalchemy.types.VARCHAR(length=255),
                        'created_at':sqlalchemy.types.VARCHAR(length=255),
                        'id': sqlalchemy.types.VARCHAR(length=255),
                        'text': sqlalchemy.types.VARCHAR(length=5000),
                        'sentiment_score': sqlalchemy.types.Float(precision=3, asdecimal=True),
                        'sentiment':sqlalchemy.types.VARCHAR(length=255),
                           })
    # Location information in tweet. Currently not in use.
    # location.to_sql('location_data', redshift_conn, index=False, if_exists='replace')

    # Can comment away print statement for actual deployment. Left here so that status will be printed in
    # console for demo purpose.
    print("Redshift DB updated successfully.")


if __name__ == "__main__":
    main(sys.argv[1])
