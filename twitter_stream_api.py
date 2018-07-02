#!/usr/bin/env python

"""
Pre-requisites:
For this application we are using tweepy. Install it if required. 
$ pip install tweepy

Sign up for an app in https://apps.twitter.com/
"""

import tweepy
import json

# Retrieve the consumer keys and secrets from apps.twitter.com
consumer_key = "6IqHdMv..."
consumer_secret = "uvgQG..."

access_token = "15101127-AY4CW..."
access_token_secret = "DEPgC5Ue..."

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.text)
        message = json.dumps(status._json).encode("utf-8")
        with open("tweets/%d.json" % status.id, "wb") as f:
          f.write(message)
        
    def on_error(self, status_code):
        print("Error code: ", status_code)
        
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

search_terms = ["trump"]
myStream.filter(track=search_terms)
