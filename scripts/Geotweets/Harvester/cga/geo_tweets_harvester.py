
# coding: utf-8

# In[1]:

import time
from datetime import datetime
import sys
import subprocess
import math
import threading
import os 
import imp
import csv
from datetime import datetime
import json
import os
import gc
import requests 
from twitter import *


# In[2]:

def authenticateUser (userName, appInfo):
    if (userName == '***'):
        oauthToken_user_1 = '***'
        oauthSecret_user_1 = '***'
        return oauthToken_user_1, oauthSecret_user_1
    
    elif (userName == '***'):
        oauthToken_user_2 = '***'
        oauthSecret_user_2 =  '***'
        return oauthToken_user_2, oauthSecret_user_2
        
    elif (userName == '***'):
        oauthToken_user_3 = '***'
        oauthSecret_user_3 =  '***'
        return oauthToken_user_3, oauthSecret_user_3
        

    elif (userName == '***'):
        oauthToken_user_4 = '***'
        oauthSecret_user_4 =  '***'
        return oauthToken_user_4, oauthSecret_user_4
        

    elif (userName == '***'):
        oauthToken_user_5 = '***'
        oauthSecret_user_5 =  '***'
        return oauthToken_user_5, oauthSecret_user_5


# In[3]:

def conv4326To900913 (lonLat): #expects tuple of x,y
    outCoords = [0.0] * 2 
    outCoords[0] = lonLat[0] * 111319.490778
    outCoords[1] = 57.295779513083 * math.log(math.tan(.0087266462599717*lonLat[1] + .78539816339745)) 
    outCoords[1] *= 111319.490778
    return outCoords


# In[ ]:

class Harvester (threading.Thread):
    maxErrors = 10
    def __init__(self, username, harvName, boundingBox, appInfo): #boundingBox is tuple in form (minX, minY, maxX, maxY)
        super(Harvester,self).__init__()
        self.username = username
        self.harvName = harvName
        self.boundingBox = boundingBox # should be tuple of len 4
        self.location = str(self.boundingBox[0]) + "," + str(self.boundingBox[1]) + "," +  str(self.boundingBox[2]) + "," + str(self.boundingBox[3])
        self.daemon = True
        self.appInfo = appInfo
        self.token, self.secret = authenticateUser(self.username, self.appInfo) 

    def run(self):
        time.sleep(5)
        twitterStream = TwitterStream(auth=OAuth(self.token, self.secret, self.appInfo["key"], self.appInfo["secret"]))
        streamIt = twitterStream.statuses.filter(locations=self.location)
        errorCount = 0
        #c=0
        tweets_dict = {}
        dirname = '/data/geotweets_cga/geotweets_data'
        while errorCount < 10: 
            try:
                for tweet in streamIt:
                            #print("fetched tweet ", tweet)
                            date= datetime.now().strftime('%Y-%m-%d %H %M')
                            minute = date[14:16]
                            min_bck = str(59)
                            #time.sleep(5)
                            fieldnames = ['tweet_id', 'time','lat','lon','goog_x','goog_y','sender_id','sender_name','source','reply_to_user_id','reply_to_tweet_id','place_id','tweet_text','lang']
                            if(minute == min_bck):
                                #time.sleeo(5)
                                #print("Inside writing loop")
                                year = date[:4]
                                month= date[5:7]
                                day= date[8:10]
                                hour = date[11:13]
                                DHG = year + '_' + month + '_' + day + '_' + hour
                                fname = "geo_tweets_hour_" + DHG + ".csv"
                                #print(fname)
                                file_exists = os.path.isfile(os.path.join(dirname, fname))
                                f = open(os.path.join(dirname, fname),'a+',encoding='utf-8')
                                writer = csv.DictWriter(f, fieldnames=fieldnames,delimiter=',')
                                
                                if not file_exists:
                                    f.seek(0,0)
                                    #if(c==1):
                                    #   continue
                                    #if(c!=1):
                                    writer.writerow({'tweet_id': ('tweet_id'), 'time': ('time'), 'lat':('lat'),'lon':('lon'),'goog_x':('goog_x'),'goog_y':('goog_y'),
                                                     'sender_id':('sender_id'),'sender_name':('sender_name'),'source':('source'),'reply_to_user_id':('reply_to_user_id'),
                                                     'reply_to_tweet_id':('reply_to_tweet_id'),'place_id':('place_id'),'tweet_text':('tweet_text'),'lang':('lang')})
                                    #   c=1
                                    
                                for key in tweets_dict.keys():
                                    f.seek(0,2)
                                    #print("Writing tweet ", key)
                                    writer.writerow({'tweet_id': (key), 'time': (tweets_dict[key][0]), 'lat':(tweets_dict[key][1]),'lon':(tweets_dict[key][2]),
                                                     'goog_x':(tweets_dict[key][3]),'goog_y':(tweets_dict[key][4]),'sender_id':(tweets_dict[key][5]),
                                                     'sender_name':(tweets_dict[key][6]),'source':(tweets_dict[key][7]),
                                                     'reply_to_user_id':(tweets_dict[key][8]),'reply_to_tweet_id':(tweets_dict[key][9]),
                                                     'place_id':(tweets_dict[key][10]),'tweet_text':(tweets_dict[key][11]),'lang':(tweets_dict[key][12])})  
                                    
                                f.close()
         
                                tweets_dict.clear()
                                #c=0
                            if tweet.get('text'):
                                tweet_id = tweet.get('id', None)
                                if(tweet_id == None):
                                    continue
                                
                                rawTime = tweet['created_at']
                                structTime = time.strptime(rawTime, "%a %b %d %H:%M:%S +0000 %Y")
                                stringTime = time.strftime("%Y-%m-%d %H:%M:%S", structTime)
                                
                                geo = tweet.get('geo')
                                lat = None
                                lon = None
                                if geo != None and geo != '':
                                    lat = geo['coordinates'][0]
                                    lon = geo['coordinates'][1]
                                else:
                                    continue
                                
                                googCoords = [None] * 2
                                if lat != None and lon != None:
                                    try:
                                        googCoords = conv4326To900913((lon, lat))
                                    except Exception as x:
                                        pass # most likely b/c of math domain error
                                goog_x = googCoords[0]
                                goog_y = googCoords[1]
                                
                                user = tweet['user']
                                if user == None:
                                    user = {}
                                sender_id=user.get('id')
                                sender_name =user.get('screen_name')
    
                                source = tweet.get('source')
                                if source != None:
                                    if source.count('<') == 2 and source.count('>') == 2: 
                                        startInner = source.find('>') + 1
                                        endInner = source.find('<', startInner)
                                        source = source[startInner:endInner]
                                
                                reply_to_user_id = tweet.get('in_reply_to_user_id')
                                reply_to_tweet_id = tweet.get('in_reply_to_status_id')
                                
                                
                                place = tweet.get('place')
                                if place == None:
                                    place = {}
                                place_id = place.get('id', '')
                                
                                tweetText= tweet.get('text')
                                tweet_text = tweetText.replace('\n', ' ').replace('\t', ' ')
                                
                                lang= tweet.get('lang')
                
                                tweets_dict[tweet_id] = [stringTime, lat, lon, goog_x, goog_y, sender_id, sender_name,
                                                         source,reply_to_user_id,reply_to_tweet_id,place_id,tweet_text,lang]
            except:
                print ("Disconnected from Twitter: ",x)
                errorCount += 1
                time.sleep((2 * errorCount) ** 2 ) #exponentially back off
                sys.exit()
            
            gc.collect()
                    


# In[ ]:

def main():
    appParams = {}
    userParams = []
    try:
        # App parameters
        appParams["name"] = 'Twerld'
        appParams["key"] = 'rIg8zjOpPcvGgWbteJYQ'
        appParams["secret"] = 'nXXc7wLxH7ONCNFEDrpNW7T0qGMkpUQfoLUXNhG1to'

        #Bounding boxes
        bounding_box_1 = 'NWW,databases4life,-180,15,-92,90'
        lyst_bb_1 = bounding_box_1.split(',')

        bounding_box_2 = 'NWE,gamalabdalwahid,-92,15,-30,90'
        lyst_bb_2 = bounding_box_2.split(',')

        bounding_box_3 = 'SW,Velos_MapD,-180,-90,-30,15'
        lyst_bb_3 = bounding_box_3.split(',')

        bounding_box_4 = 'EW,kedahek,-30,-90,60,90'
        lyst_bb_4 = bounding_box_4.split(',')

        bounding_box_5 = 'EE,tmostak,60,-90,180,90'
        lyst_bb_5 = bounding_box_5.split(',')


       # User parameters
        userParams=[[lyst_bb_1[0], lyst_bb_1[1], lyst_bb_1[2], lyst_bb_1[3], lyst_bb_1[4], lyst_bb_1[5]], [lyst_bb_2[0], lyst_bb_2[1], lyst_bb_2[2], lyst_bb_2[3], lyst_bb_2[4], lyst_bb_2[5]], [lyst_bb_3[0], lyst_bb_3[1], lyst_bb_3[2], lyst_bb_3[3], lyst_bb_3[4], lyst_bb_3[5]], [lyst_bb_1[0], lyst_bb_4[1], lyst_bb_4[2], lyst_bb_4[3], lyst_bb_4[4], lyst_bb_4[5]],[lyst_bb_5[0], lyst_bb_5[1], lyst_bb_5[2], lyst_bb_5[3], lyst_bb_5[4], lyst_bb_5[5]]]
        
    except Exception as x:
        sys.exit(1)
    harvs = []
    for p in userParams:
       harvs.append(Harvester(p[1], p[0], (p[2], p[3], p[4], p[5]), appParams)) 

    numHarvs = len(harvs)
 
    for h in harvs:
        h.start()
        
    while len(harvs) > 0: 
        try:
            for h in range(numHarvs):
                harvs[h].join(1)
                if harvs[h].isAlive() == False:
                    harvs[h] = Harvester(userParams[h][1], userParams[h][0], (userParams[h][2], userParams[h][3], userParams[h][4], userParams[h][5]), appParams)
                    harvs[h].start()
        except KeyboardInterrupt: #allows us to cntl-c
            sys.exit(2)
            
    
# Calling name    
if __name__ == "__main__":
    main()


# In[ ]:



