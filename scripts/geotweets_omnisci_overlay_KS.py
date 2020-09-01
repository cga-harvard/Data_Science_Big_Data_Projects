import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np


path = '/n/holyscratch01/cga/dkakkar/data/geotweets/2018/' # use your path
all_files = glob.glob(path + "/*.gz")
l_gf_ni=[]

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9563, dbname="omnisci") #use your port number
print("Connected",conn)

l_ni=[]
conn.execute("DROP TABLE IF EXISTS geotweets;")
conn.execute("Drop table if exists geotweets_fips;")
conn.execute("Create table IF NOT EXISTS geotweets_fips (message_id BIGINT,tweet_date TIMESTAMP,tweet_text TEXT,tags TEXT ENCODING DICT(32),tweet_lang TEXT ENCODING DICT(32),source TEXT ENCODING DICT(32),place TEXT, retweets BIGINT, tweet_favorites BIGINT,photo_url TEXT ENCODING DICT(32),quoted_status_id BIGINT,user_id BIGINT,user_name TEXT,user_location TEXT,followers BIGINT,friends BIGINT,user_favorites BIGINT,status BIGINT,user_lang TEXT ENCODING DICT(32),latitude DOUBLE,longitude DOUBLE,data_source TEXT ENCODING DICT(32),GPS TEXT ENCODING DICT(32),spatialerror DOUBLE,rowid0 BIGINT,FIPS text);")
for filename in all_files:
    print(filename)
    try:
      df = pd.read_csv(filename, sep='\t',dtype='unicode',index_col=None, low_memory='true',compression='gzip')
    except:
      l_ni.append(filename)
      continue

    #df.drop(['geom', 'tags', 'source','place','tweet_favorites','photo_url','quoted_status_id','user_id','user_name','user_location','followers','friends','user_favorites','status','user_lang','data_source'],axis=1, inplace=True) 
    df = df.drop(['geom'], axis = 'columns')
    df.columns=['message_id','tweet_date','tweet_text','tags','tweet_lang','source','place','retweets','tweet_favorite', 'photo_url','quoted_status_id','user_id','user_name','user_location','followers','friends','user_favorites','status','user_lang','latitude','longitude','data_source','GPS','spatialerror'] 
    cols = ['message_id', 'retweets', 'tweet_favorite', 'quoted_status_id', 'user_id',
               'followers','friends', 'user_favorites','status','latitude', 'longitude',
               'spatialerror']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df['tweet_date'] = pd.to_datetime(df['tweet_date'], errors='coerce')
    values = {'message_id':0, 'retweets':0, 'tweet_favorites':0, 'quoted_status_id':0, 'user_id':0,
               'followers':0,'friends':0, 'user_favorites':0,'status':0,'latitude':0, 'longitude':0,
               'spatialerror':0}
    df.fillna(value=values, inplace = True)
    #print(df.head())
    try:
       conn.load_table("geotweets",df,create='infer',method='arrow')

    except:
       try:
          conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP,tweet_text TEXT,tags TEXT ENCODING DICT(32),tweet_lang TEXT ENCODING DICT(32),source TEXT ENCODING DICT(32),place TEXT, retweets BIGINT, tweet_favorites BIGINT,photo_url TEXT ENCODING DICT(32),quoted_status_id BIGINT,user_id BIGINT,user_name TEXT,user_location TEXT,followers BIGINT,friends BIGINT,user_favorites BIGINT,status BIGINT,user_lang TEXT ENCODING DICT(32),latitude DOUBLE,longitude DOUBLE,data_source TEXT ENCODING DICT(32),GPS TEXT ENCODING DICT(32),spatialerror DOUBLE);") 
          conn.load_table_columnar("geotweets", df,preserve_index=False) 
       
       except:
         
         l_ni.append(filename)
         
         continue
         #conn.execute("Drop table if exists geotweets_fips")
    try:
       #print("Inside loading geotweets_fips", filename)
       conn.execute("INSERT INTO geotweets_fips (Select a.*,b.fips from geotweets a inner join KS b ON ST_Intersects(b.omnisci_geo,ST_SetSRID(ST_Point(a.longitude,a.latitude),4326)));")
       #conn.execute("Drop table if exists geotweets;")
    except:
       print("Not loaded geotweets_fips")
       l_gf_ni.append(filename)
    conn.execute("Drop table if exists geotweets;")
#conn.execute("Select count(*) from geotweets_fips;") 
    #conn.execute("Drop table if exists geotweets;")
#print(l_gf_ni)
#print(l_ni)
with open('logfile.txt', 'w') as filehandle:
    filehandle.writelines("%s\n" % name for name in l_gf_ni)
filehandle.close()
