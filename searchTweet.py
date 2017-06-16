#! /usr/bin/env python3
# coding: utf-8

from TwitterSearch import *
from functools import reduce
import datetime
import json
import sys
from kafka import KafkaProducer

consumerKey = "3Ax78wxvjlrboTUXx5eHnWayU"
consumerSecret = "GVo6WDH1ay3vhuAeosICCskCQ1nV2VQN62ZZagqUV6DLRSn7Ah"
accessTokenKey = "860356285191028736-DwOiGkvW1LGocxRweFnpS9XtlBNLEzM"
accessTokenSecret = "JqeLKXi3Ej2o6dPhYiXAJVRmASRQc73P71ODKvkMFfEmb"

def searchTweet(mySearch, lang, isSaved, filePath, isSentToKafka, kafkaProd, topic):
    try:
        now = str(datetime.datetime.now()).replace(" ","T")
        if isSaved:
            fd = open(filePath, 'w')
        tso = TwitterSearchOrder() # create a TwitterSearchOrder object
        tso.set_keywords(mySearch) # let's define all words we would like to have a look for
        tso.set_language(lang) # we want to see German tweets only
        tso.set_include_entities(False) # and don't give us all those entity information

        # it's about time to create a TwitterSearch object with our secret tokens
        ts = TwitterSearch(
            consumer_key = consumerKey,
            consumer_secret = consumerSecret,
            access_token = accessTokenKey,
            access_token_secret = accessTokenSecret)
        
         # this is where the fun actually starts :)
        for tweet in ts.search_tweets_iterable(tso):
            data = {"id": tweet["id"],
                    "user": tweet["user"]["screen_name"],
                    "sentence": tweet["text"],
                    "country": getCountry(tweet["place"]),
                    "lang": tweet["lang"],
                    "retweetCount": tweet["retweet_count"],
                    "isRetweet": tweet["retweeted"],
                    "keySearch": mySearch[0],
                    "eventDate": now
            }
            if isSaved:
#                fd.write(json.dumps(data).encode("utf-8")+"\n")
#                fd.flush()
                pass
            if isSentToKafka:
                kafkaProd.send(topic, json.dumps(data).encode("utf-8"))
        return ts.search_tweets(tso)

    except TwitterSearchException as e: # take care of all those ugly errors if there are some
        print(e)

def getCountry(place):
    try:
        return place["name"]
    except Exception:
        return "none"

def countTweet(mySearch):
    return len(searchTweet(mySearch)["content"]["statuses"])

def help():
    print("Options:")
    print("\t-k\tsearch keyword")
    print("\t\tEx: -kSpark")
    print("\t-f\tgive a filename to save in Json")
    print("\t\tEx -fMySearchResults.json")
    print("\t-t\tgive topic to send with Kafka")
    print("\t\t-tTweet")

if __name__=="__main__":
    key = "spark"
    producer = ""
    lang = "en"
    topic = ""
    isSaved = True
    filePath = "tweets_on_{}".format(key)
    isSentToKafka = False
    for val in sys.argv:
        if val.startswith("-k"):
            key = val[2:]
        if val.startswith("-f"):
            isSaved = True
            filePath = val[2:]
        if val.startswith("-l"):
            lang = val[2:]
        if val.startswith("-t"):
            try:
                producer = KafkaProducer(bootstrap_servers="localhost:9092")
                isSentToKafka = True
                topic = val[2:]
            except Exception:
                print("Kafka not available\nCheck your connexion")
    if any("--help" in s for s in sys.argv):
        help()
    else:
        searchTweet([key], lang, isSaved, filePath, isSentToKafka, producer, topic)
