#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Geotweet harvester v2.0
# Author: Devika Kakkar
#Date:04/21/21

import sys
import tweepy
import pickle
import os
import csv
import time
import datetime
import subprocess
import threading
import imp
import json
import gc
import math
import requests 
from twitter import *

CONSUMER_KEY = 'cwcRS6DJfvI8f8mKmaSjJr4W6'
CONSUMER_SECRET = 'fObJI83jfoGEMwGDApcStyCP5ISjG0wvmLxhRQkZrcXtLKqRF1'
ACCESS_KEY = '1381805353504026627-l057EhKljyMoDxq0b22ST175UyZAHv'
ACCESS_SECRET = 'knUov3fothAbcSg8itpPtuOG4yEBkLrQQbVmfbkzxSALB'


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
tweets_dict = {}
dirname = '/data/geotweets_cga/geotweets_data'
fieldnames =['tweet_id', 'time','lat','lon','goog_x','goog_y','sender_id','sender_name','source','reply_to_user_id','reply_to_tweet_id','place_id','tweet_text','lang']
min_bck = str(59)

def conv4326To900913 (lonLat): #expects tuple of x,y
    outCoords = [0.0] * 2 
    outCoords[0] = lonLat[0] * 111319.490778
    outCoords[1] = 57.295779513083 * math.log(math.tan(.0087266462599717*lonLat[1] + .78539816339745)) 
    outCoords[1] *= 111319.490778
    return outCoords



# In[ ]:



class CustomStreamListener(tweepy.StreamListener):
 
    def on_status(self, status):
        try:
            if status.coordinates!=None:
                            
                            if status.text:
                                tweet_id = status.id
                                #print(tweet_id)                     
                                rawTime = status.created_at 
                                lat = None
                                lon = None
                                
                                lat = status.coordinates['coordinates'][0]
                                lon = status.coordinates['coordinates'][1]
                                googCoords = [None] * 2
                                if lat != None and lon != None:
                                    try:
                                        googCoords = conv4326To900913((lon, lat))
        
                                    except Exception as x:
                                        pass # most likely b/c of math domain error
                
                                goog_x = googCoords[0]
                                goog_y = googCoords[1]
                                
                                user = status.user
                                if user == None:
                                    user = {}
                                sender_id=user.id
                                sender_name =user.screen_name
                                source = status.source
                                if source != None:
                                    if source.count('<') == 2 and source.count('>') == 2: 
                                        startInner = source.find('>') + 1
                                        endInner = source.find('<', startInner)
                                        source = source[startInner:endInner]
                                
                                reply_to_user_id = status.in_reply_to_user_id
                                reply_to_tweet_id = status.in_reply_to_status_id
                                place = status.place
                                if place == None:
                                    place = {}
                                place_id = place.id
                                tweetText= status.text         
                                tweet_text = tweetText.replace('\n', ' ').replace('\t', ' ')
                                lang= status.lang
                                #print(lang)
                                tweets_dict[tweet_id] = [rawTime, lat, lon, goog_x, goog_y, sender_id, sender_name,
                                                         source,reply_to_user_id,reply_to_tweet_id,place_id,tweet_text,lang]
                                
                                date= datetime.datetime.now().strftime('%Y-%m-%d %H %M')
                                minute = date[14:16]
                                
                                if(minute == min_bck):
                                    #print("Inside writing loop")
                                    year = date[:4]
                                    month= date[5:7]
                                    day= date[8:10]
                                    hour = date[11:13]
                                    DHG = year + '_' + month + '_' + day + '_' + hour
                                    fname = "geo_tweets_hour_" + DHG + ".csv"
                                    #print(fname)
                                    file_exists = os.path.isfile(os.path.join(dirname, fname))
                                    #print(file_exists)
                                    f = open(os.path.join(dirname, fname),'a+',encoding='utf-8')
                                    writer = csv.DictWriter(f, fieldnames=fieldnames,delimiter=',')
             
                                    if (not file_exists):
                                        #print("Inside header")
                                        f.seek(0,0)
                                        writer.writerow({'tweet_id': ('tweet_id'), 'time': ('time'), 'lat':('lat'),'lon':('lon'),'goog_x':('goog_x'),'goog_y':('goog_y'),
                                                         'sender_id':('sender_id'),'sender_name':('sender_name'),'source':('source'),'reply_to_user_id':('reply_to_user_id'),
                                                         'reply_to_tweet_id':('reply_to_tweet_id'),'place_id':('place_id'),'tweet_text':('tweet_text'),'lang':('lang')})
                                        #   c=1

                                    for key in tweets_dict.keys():
                                        #print("Inside tweet writing")
                                        f.seek(0,2)
                                        writer.writerow({'tweet_id': (key), 'time': (tweets_dict[key][0]), 'lat':(tweets_dict[key][1]),'lon':(tweets_dict[key][2]),
                                                         'goog_x':(tweets_dict[key][3]),'goog_y':(tweets_dict[key][4]),'sender_id':(tweets_dict[key][5]),
                                                         'sender_name':(tweets_dict[key][6]),'source':(tweets_dict[key][7]),
                                                         'reply_to_user_id':(tweets_dict[key][8]),'reply_to_tweet_id':(tweets_dict[key][9]),
                                                         'place_id':(tweets_dict[key][10]),'tweet_text':(tweets_dict[key][11]),'lang':(tweets_dict[key][12])})  

                                    f.close()

                                    tweets_dict.clear()

        except:
            print (sys.stderr, 'Encountered Exception:',e)
            print (datetime.datetime.now())
            pass

    def on_error(self, status_code):
        print (sys.stderr, 'Encountered error with status code:', status_code)
        print (datetime.datetime.now())
        return True # Don't kill the stream

    def on_timeout(self):
        print (sys.stderr, 'Timeout...')
        print (datetime.datetime.now())
        return True # Don't kill the stream

# Create a streaming API and set a timeout value of 60 seconds

while True:
    try: 
        streaming_api1 = tweepy.streaming.Stream(auth, CustomStreamListener(), timeout=30)
        
        print (sys.stderr, 'Filtering the public timeline for ')
        

        streaming_api1.filter(locations=[-180,-90,180,90])
    except:
                print (sys.stderr, 'Encountered Exception:')
                print (datetime.datetime.now())
                pass


# In[ ]:




