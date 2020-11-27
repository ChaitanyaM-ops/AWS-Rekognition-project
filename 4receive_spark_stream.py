r"""
 Use DataFrames and SQL to process the JSON string in UTF8 encoded, '\n' delimited text received from the
 network every second.
"""
import json
import pandas as pd
import subprocess

import requests
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

import boto
from boto.s3.key import Key

from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import explode

# Update the Amazon keys here. Alternatively, one can use environment variables or save the credentials
# in a credential configuration file and place it in the AWS default folder. For windows, the location is
# C:\Users\<username>\.aws\ and the file should be named credentials with no file extension.
# Placing the API key here is not a good practice. This is to facilitate other users in running this
# script subsequently.

key_id=''
secret_key=''
my_region=''
bucket = "bead-project-target-upload-bucket"

# Initialise the AWS S3 connection. If using environment variable or credential configuration file, one can
# just remove the variable assignment, e.g. "=key_id", "=secret_key".
s3 = boto3.client('s3',
                  aws_access_key_id=key_id,
                  aws_secret_access_key=secret_key)

# Create AWS session
session = boto3.Session(aws_access_key_id=key_id,
                        aws_secret_access_key=secret_key,
                        region_name=my_region)

# Connect to Amazon Rekognition
rekognition = boto3.client('rekognition',
                           aws_access_key_id=key_id,
                           aws_secret_access_key=secret_key,
                           region_name=my_region)

# Connect to Amazon Redshift
redshift_conn = create_engine('postgresql+psycopg2://<username>:<Redshift DB URL>:5439/dev')

# setup the bucket for direct file upload to S3 bucket from image url
c = boto.connect_s3(key_id, secret_key)
b = c.get_bucket(bucket, validate=False)
k = Key(b)

# The reference image for facial recognition and comparison. One will upload the image, Reference_Image.jpeg to
# S3 bucket inside the reference folder.
reference_image = "reference/Reference_Image.jpg"

# Function to retrieve current Spark Session
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

# This function will call Amazon Rekognition to see if the image has a face first as otherwise, the compare_face
# function from Rekognition will throw an exception.
def detect_faces(bucket, key):
    response = rekognition.detect_faces(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    return len(response['FaceDetails'])

# This function will call Amazon Rekognition to compare the images with faces in the tweet with the target
# image that we want. Source image is the image of the person of interest for it to compare to. Target image
# in this case is the image received from Twitter. If the threshold is not specified when called, the default
# will be 80%.
def compare_faces(bucket, sourcekey, targetkey, threshold=80, username=""):
    # For actual deployment, one can comment away the print statement. This is left here so that one can see
    # the progress in the console for demo.
    print("Face detected. Starting Face Comparison function")

    # Comparing the two images.
    response = rekognition.compare_faces(
        SourceImage={
            'S3Object': {
                'Bucket': bucket,
                'Name': sourcekey
            }
        },
        TargetImage={
            'S3Object': {
                'Bucket': bucket,
                'Name': targetkey
            }
        },
        SimilarityThreshold=threshold
    )

    for faceMatch in response['FaceMatches']:
        similarity = str(faceMatch['Similarity'])

        # Call another script to retrieve the user details of the Twitter that posted a matching photo.
        subprocess.call(['python', '5get_target_User_tweets.py', "" + username])

        # One can comment away the print statement for actual deployment. This is left here so that one can see the
        # comparison result in the console for demo.
        print("Similarity = " + similarity)

    return similarity

# Function to directly download the Tweet image to Amazon S3 bucket
def alt_S3_upload(image_url, destination):
    r = requests.get(image_url)
    # upload the file
    k.key = destination
    k.content_type = r.headers['content-type']
    k.set_contents_from_string(r.content)

# Convert RDDs of the JSON string DStream to DataFrame and run SQL query
def process(time, rdd):
    # One can comment away the below print statement. This is left here to show that it's running via
    # console printout for demo purpose.
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession. May not need for our use case
        spark = getSparkSessionInstance(rdd.context.getConf())

        df = (sqlContext.read.option("inferSchema", True).json(rdd))

        # Expanding the Twitter objects as some are nested, and renaming the "columns".
        new_df = df.select("data.author_id","data.id","data.text", "data.created_at",
                           explode(df["includes.media"]),df["includes.users"].getItem(0))\
            .toDF("author_id", "id", "text", "date", "media", "users")

        # Extract the fields required
        subset_df = new_df.select("author_id","id","text","date","media.media_key","media.url","users.created_at","users.description",
                      "users.name","users.username","users.verified","users.public_metrics.followers_count",
                      "users.public_metrics.following_count","users.public_metrics.listed_count","users.public_metrics.tweet_count")

        # Convert to normal Panda dataframe and extract all the fields to prepare to download image, and to send to
        # DynamoDB
        normal_df = subset_df.toPandas()

        for index, row in normal_df.iterrows():
            # Can comment away the print statement for actual deployment.
            print(f"Index in iterrows: {index}")

            # The media_key for the image will be used as primary id. Filename of the image will be appended with
            # part of the url hosting the image as well.
            id = row["media_key"]
            filename = id + "---"+ row["url"].split("/")[-1]
            image_url = row["url"]

            # S3 destination is a folder named images. This combination of folder and filename will be the
            # S3 object key.
            destination = "images/" + filename

            # Downloading the image directly to Amazon S3 bucket
            alt_S3_upload(image_url,destination)

            # Creating the JSON entry to store in Dynamo DB/ Amazon Redshift.
            dynamoItem={
                'id': id,
                'author_id': row["author_id"],
                'description': row["description"],
                'date': row["date"],
                'text': row["text"],
                'image_url': image_url,
                'created': row["created_at"],
                'username': row["username"],
                'name': row["name"],
                'verified': row["verified"],
                'followers_count': row["followers_count"],
                'following_count': row["following_count"],
                'listed_count':row["listed_count"],
                'tweet_count':row["tweet_count"],
                's3_url':bucket + '/' + destination
            }

            # destination is the image received from Twitter and stored onto S3 bucket
            if detect_faces(bucket, destination) <= 0:
                s3.delete_object(Bucket=bucket, Key=destination)
                print("No faces detected in " + destination + ". Deleting...")
            else:
                response = compare_faces(bucket, reference_image, destination, 70, row["username"])

                # If response is empty, means not a match, can delete the S3 object the dynamodb entry
                if response == "":
                    s3.delete_object(Bucket=bucket, Key=destination)
                else:
                    # Save the tweet details to Redshift DB
                    tweetmatch_df = pd.DataFrame(dynamoItem, index=[0])
                    tweetmatch_df["similarity"] = response

                    # Update the tweet information to Amazon Redshift. Record is appended so that all
                    # matching tweets are recorded.
                    tweetmatch_df.to_sql('tweet_match', redshift_conn, index=False, if_exists='append')
    except:
        pass

# Main to initialise the Spark Context and Streaming Context
if __name__ == "__main__":

    # Start the Spark StreamingContext to receive the DStream containing the tweet JSON objects
    sc = SparkContext(appName="BEADTwitterFaceRecognition")
    ssc = StreamingContext(sc, 1)
    sqlContext = SQLContext(sc)
    lines = ssc.socketTextStream("localhost", 9999)
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
