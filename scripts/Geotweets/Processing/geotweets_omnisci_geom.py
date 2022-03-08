import pandas as pd
#import geopandas
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np


path = '/n/holyscratch01/cga/dkakkar/data/geotweets/results/2020/' # use your path
all_files = glob.glob(path + "/*.gz")

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=8558, dbname="omnisci") #use your port number
print("Connected",conn)
#query="DROP TABLE IF EXISTS geotweets"
#coinn.execute(query)
l_ni=[]
conn.execute("DROP TABLE IF EXISTS geotweets;")

for filename in all_files:
    #print(filename)
    try:
      df = pd.read_csv(filename, sep='\t',dtype='unicode',index_col=None, low_memory='true',compression='gzip')
    except:
      l_ni.append(filename)
      #print("Corrupt file",filename)
      continue
    df = df.drop(['geom'], axis = 'columns')
    #df['geom']=geopandas.points_from_xy(df.longitude, df.latitude)
    #print(df.head())
    #df.columns=['GLOBALEVENTID','SQLDATE','MonthYear','Years','FractionDate','Actor1Code','Actor1Name','Actor1CountryCode','Actor1KnownGroupCode','Actor1EthnicCode','Actor1Religion1Code','Actor1Religion2Code','Actor1Type1Code','Actor1Type2Code','Actor1Type3Code','Actor2Code','Actor2Name','Actor2CountryCode','Actor2KnownGroupCode','Actor2EthnicCode','Actor2Religion1Code','Actor2Religion2Code','Actor2Type1Code','Actor2Type2Code','Actor2Type3Code','IsRootEvent','EventCode','EventBaseCode','EventRootCode','QuadClass','GoldsteinScale','NumMentions','NumSources','NumArticles','AvgTone','Actor1Geo_Type','Actor1Geo_FullName','Actor1Geo_CountryCode','Actor1Geo_ADM1Code','Actor1Geo_Lat','Actor1Geo_Long','Actor1Geo_FeatureID','Actor2Geo_Type','Actor2Geo_FullName','Actor2Geo_CountryCode','Actor2Geo_ADM1Code','Actor2Geo_Lat','Actor2Geo_Long','Actor2Geo_FeatureID','ActionGeo_Type','ActionGeo_FullName','ActionG]    #print(df.head(5))    #li.append(df)

#frame = pd.concat(li, axis=0, ignore_index=True)
    df.columns=['message_id','tweet_date','tweet_text','tags','tweet_lang','source','place','retweets','tweet_favorite', 'photo_url','quoted_status_id','user_id','user_name','user_location','followers','friends','user_favorites','status','user_lang','latitude','longitude','data_source','GPS','spatialerror'] 
    cols = ['message_id', 'retweets', 'tweet_favorite', 'quoted_status_id', 'user_id',
               'followers','friends', 'user_favorites','status','user_lang','latitude', 'longitude',
               'spatialerror']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    #df['GPS'] = df['GPS'].astype('bool')
    df['tweet_date'] = pd.to_datetime(df['tweet_date'], errors='coerce')
    values = {'message_id':0, 'retweets':0, 'tweet_favorites':0, 'quoted_status_id':0, 'user_id':0,
               'followers':0,'friends':0, 'user_favorites':0,'status':0,'latitude':0, 'longitude':0,
               'spatialerror':0}
    df.fillna(value=values, inplace = True)
    #print(list(df.columns.values))
#frame.to_parquet('/n/holyscratch01/cga/dkakkar/data/gdelt.parquet.gzip',compression='gzip')
    try:
       conn.load_table("geotweets",df,create='infer',method='arrow')
       #print("Inserted Arrow", filename)
    #conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP(0),tweet_text TEXT ENCODING NONE,tags TEXT ENCODING DICT(32),tweet_lang TEXT ENCODING DICT(32),source TEXT ENCODING DICT(32),place TEXT ENCODING NONE, retweets SMALLINT, tweet_favorites SMALLINT,photo_url TEXT ENCODING DICT(32),quoted_status_id BIGINT,user_id BIGINT,user_name TEXT ENCODING NONE,user_location TEXT ENCODING NONE,followers SMALLINT,friends SMALLINT,user_favorites INT,status INT,user_lang TEXT ENCODING DICT(32),latitude FLOAT,longitude FLOAT,data_source TEXT ENCODING DICT(32),GPS TEXT ENCODING DICT(32),spatialerror FLOAT);")

    #conn.load_table_columnar("geotweets", df,preserve_index=False)    
      #print("Inserted", filename)
    except:
       try:
         conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP(0),tweet_text TEXT ENCODING NONE,tags TEXT ENCODING DICT(32),tweet_lang TEXT ENCODING DICT(32),source TEXT ENCODING DICT(32),place TEXT ENCODING NONE, retweets SMALLINT, tweet_favorite SMALLINT,photo_url TEXT ENCODING DICT(32),quoted_status_id BIGINT,user_id BIGINT,user_name TEXT ENCODING NONE,user_location TEXT ENCODING NONE,followers SMALLINT,friends SMALLINT,user_favorites INT,status INT,user_lang TEXT ENCODING DICT(32),latitude FLOAT,longitude FLOAT,data_source TEXT ENCODING DICT(32),GPS TEXT ENCODING DICT(32),spatialerror FLOAT);")
         conn.load_table_columnar("geotweets", df,preserve_index=False) 
         #print ("Inserted columnar", filename) 
       except:
         #print("Inside except")
         l_ni.append(filename)
         #print("Not inserted",filename)
         continue
conn.execute("Create table geotweets_geom AS (Select message_id, tweet_date, tweet_text, tags, tweet_lang, source, place, retweets, tweet_favorite, photo_url,quoted_status_id, user_id, user_name, user_location, followers, friends, user_favorites, status, user_lang, latitude, longitude, data_source, GPS, spatialerror, ST_Point(longitude, latitude) as geom from geotweets)")
 
#print(l_ni)
conn.execute("DROP TABLE IF EXISTS geotweets;")
