from concurrent.futures import process
import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np
from tqdm import tqdm
from datetime import date,datetime,timedelta
import os

print("Connecting to Omnisci")
conn=connect(user="admin", password="cg@sh1re", host="localhost",
              port=6274,
              dbname="omnisci") #use your port number
print("Connected",conn)


s2 = ['message_id', 'tweet_date', 'tweet_text', 'tags', 'tweet_lang', 'source', 'place',
        'retweets', 'tweet_favorites', 'photo_url', 'quoted_status_id',
       'user_id', 'user_name', 'user_location', 'followers', 'friends',
       'user_favorites', 'status', 'user_lang', 'latitude', 'longitude',
       'data_source', 'GPS', 'spatialerror', 'sentiment_score']
values = {'message_id':0, 'retweets':0, 'tweet_favorites':0, 'quoted_status_id':0, 'user_id':0,
        'followers':0,'friends':0, 'user_favorites':0,'status':0,'latitude':0, 'longitude':0,
        'spatialerror':0}



# how check if already uploaded,
# 1. save a file lists
# 2. just do that day, actually is one day before?

now = datetime.now()
date_need_processed = now - timedelta(days=1)

path = '/data/geotweets/sentiment_tweets_output/parquet_files/2022/' # use your path
all_files = glob.glob(path + "/*.parquet")


files_need_process = []
for item in os.listdir(path):
    # [int(i) for i in item.split("_")[:-1]]
    date_ = date(*[int(i) for i in item.split("_")[:-1]])
    if date_ == date_need_processed.date():
        files_need_process.append(path + item)

        

l_ni=[]
# start = 0
for i,filename in tqdm(enumerate(files_need_process),total=len(files_need_process)):
    # print(i)
    # if i < start:
    #     continue

    try:
    # df = pd.read_csv(filename,sep=""",""",lineterminator='\n',index_col=None)
        df = pd.read_parquet(filename,engine="pyarrow")


    # if df.shape[1] == 28:
    #   break
    except Exception as e:
        print("#_#",e)
        # break
        l_ni.append(filename)
        continue

    df["message_id"] = df.index
    df = df[s2]
    df.fillna(value=values, inplace = True)

    df = df[['message_id', 'tweet_date', 'tweet_text', 'tweet_lang',
        'latitude', 'longitude',  'sentiment_score']]
    df.index = range(df.shape[0])
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['sentiment_score'] = df['sentiment_score'].astype(float)
    # # print(df.dtypes)


    try:
        conn.load_table("geotweets",df,create='infer',method='arrow')
                #print("Inserted Arrow", filename)
    #conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP(0),tweet_text TEXT ENCODING NONE,tags TEXT ENCODING DICT(32),tweet_lang TEXT ENCODING DICT(32),source TEXT ENCODING DICT(32),place TEXT ENCODING NONE, retweets SMALLINT, tweet_favorites SMALLINT,photo_url TEXT ENCODING DICT(32),quoted_status_id BIGINT,user_id BIGINT,user_name TEXT ENCODING NONE,user_location TEXT ENCODING NONE,followers SMALLINT,friends SMALLINT,user_favorites INT,status INT,user_lang TEXT ENCODING DICT(32),latitude FLOAT,longitude FLOAT,data_source TEXT ENCODING DICT(32),GPS TEXT ENCODING DICT(32),spatialerror FLOAT);")

    #conn.load_table_columnar("geotweets", df,preserve_index=False)    
    #print("Inserted", filename)
    except Exception as e:

        print(e)

    #     try:
    #         conn.execute("""Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP(0),tweet_text TEXT ENCODING NONE,
    #         tweet_lang TEXT ENCODING DICT(32),
    #         latitude FLOAT,longitude FLOAT,sentiment_score FLOAT);""")
    #         conn.load_table_columnar("geotweets", df,preserve_index=False) 
    #         #print ("Inserted columnar", filename) 
    #     except:
    #         print("Inside except")
    #         l_ni.append(filename)
    #   #print("Not inserted",filename)
    #         continue
    # break