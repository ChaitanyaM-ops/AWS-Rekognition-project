import requests
import json
import socket
import sys

# Twitter API bearer token.
bearer_token = ""

# Function to create the Twitter authentication header
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

# Function to retrieve existing rules tied to Bearer Token
def get_rules(headers):

    # GET request to retrieve existing rules tied to current Twitter account (bearer token)
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    # Printing out the response received. For actual implementation, one can comment away the print statement.
    print(json.dumps(response.json()))
    return response.json()

# Function to purge all the existing rules tied to Bearer Token
def delete_all_rules(headers, rules):
    # Check if there is any existing rule. If not, there is not need to delete the rules.
    if rules is None or "data" not in rules:
        return None

    # Retrieve the list of rules and send the POST to delete away the rules.
    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    # Print the response of the deletion request. For actual deployment, one can comment away the print statement.
    print(json.dumps(response.json()))

# Set up new rules for the Twitter monitoring
def set_rules(headers):
    # You can adjust the rules if needed. For purpose of demo, we are monitoring Twitter accounts of our own. One can
    # use keywords, and add different conditions, e.g. has video or just in general, has media. However, since our
    # solution only handles images, we have filtered for those containing images only.
    sample_rules = [
        {"value": "from:ruskyhuskysg has:images"},
        {"value": "from:PZanyu has:images"},
    ]

    # creating the monitoring rules and sending the POST command to initiate the monitoring.
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )

    # Printing the response for the addition of the monitoring rule. For actual deployment, one can comment
    # away the printing if necessary.
    print(json.dumps(response.json()))

# Function for retrieving minimal subset of response fields (for initial testing)
def get_stream_essential(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at&"
        "expansions=author_id,attachments.media_keys&user.fields=username&media.fields=type,url"
        , headers=headers, stream=True,
        )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    else:
        return response

# Function that retrieves multiple response fields, such as user details, tweet object, media object etc from
# monitoring rules set. One can add additional fields that one is interested in.
def get_stream(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,geo&"
        "expansions=author_id,attachments.media_keys,referenced_tweets.id,geo.place_id&user.fields=created_at,"
        "location,username,profile_image_url,description,public_metrics,url,verified&"
        "media.fields=type,url&place.fields=country_code,name,geo"
        , headers=headers, stream=True,
        )

    # Printing the response status code to check if it's successful. For actual deployment, one can comment
    # away this print statement.
    print(response.status_code)

    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    else:
        return response

# Function that will receive ~1% of all Twitter's public tweets. One has no control over what is
# shared by Twitter via this method.
def get_sampled_stream(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=created_at,geo&"
        "expansions=author_id,attachments.media_keys,referenced_tweets.id,geo.place_id&user.fields=created_at,"
        "location,username,profile_image_url,description,public_metrics,url,verified&"
        "media.fields=type,url&place.fields=country_code,name,geo", headers=headers, stream=True,
    )

    # Printing the response status code to check if it's successful. For actual deployment, one can comment
    # away this print statement.
    print(response.status_code)

    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    else:
        return response

# Function to send the JSON response received from Twitter to 2nd Python script via Spark Streaming
# for further processing
def send_tweets_to_spark(http_resp, tcp_connection, limit):
    count = 0 # to automatically stop the job once it hits a number since we have quota limit for our account.

    # Iterate through the lines for each response
    for line in http_resp.iter_lines():
        # Limit is added here to control the number of hits before terminating the script due to quota on
        # Twitter API and Amazon Rekognition. This can be removed for actual deployment which has paid subscription
        # or has special arrangement with Twitter to have larger quota. If one is not interested in setting any
        # quota limit, one can set the limit as -1 (or any other flags to indicate no limit).
        if count < limit or limit == -1:
            # try statement to load the response as some streams could just be kept alive data and not the actual
            # tweet data, so it will be empty
            try:
                full_tweet = json.loads(line)

                # Print statement to show what was received. For actual deployment, one can comment away this print
                # statement if not required. This is useful for showing in the console for demo.
                print(line)

                # Check if there's any media. As each tweet can contain multiple media objects, like many photos or
                # mixture of photos and  videos in one tweet, the solution needs to iterate through all and only
                # select those which are images (photos).
                if "media" in full_tweet["includes"]:
                    for m in full_tweet["includes"]["media"]:
                        # For our use case, we are matching photos, thus verify it's photo and not video
                        if m["type"] == "photo":

                            # Print statement can be commented away for actual use case. Kept here to show the progress
                            # in console for demo.
                            data = json.dumps(full_tweet)
                            tcp_connection.send(("" + data + "\n").encode("utf-8"))
                            count = count+1
            except:
                continue
        else:
            # Program will stop once limit is reached.
            break

# Main function that will initiate the monitoring of the Twitter stream and connects to Spark Streaming
def main():

    # Localhost is used for this solution but one can update it to specific IP address if setup uses a defined network
    # gateway for communication within
    TCP_IP = 'localhost'
    TCP_PORT = 9999
    conn = None

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)

    print("Waiting for TCP connection...")
    conn, addr = s.accept()

    # Creating the authentication header for Twitter
    print("Connected... Starting getting tweets.")
    headers = create_headers(bearer_token)

    # Initialization - remove all existing rules and set the current one
    rules = get_rules(headers)
    delete_all_rules(headers, rules)
    set_rules(headers)
    resp = get_stream(headers)

    # Below would be for real usage whereby we get all feeds from Twitter without matching any keywords or users
    # resp = get_sampled_stream(headers)

    # Send the tweets via Spark Streaming for further processing. First argument is the response from Twitter with the
    # JSON objects returned, second argument is the socket connection to Spark Streaming and third argument is how many
    # tweets with images to process before terminating (due to API quota limit). For actual use case, one can update with
    # the quota (e.g. 90k or others) or -1 for no limit.
    send_tweets_to_spark(resp, conn, 5)

if __name__ == "__main__":
    main()
