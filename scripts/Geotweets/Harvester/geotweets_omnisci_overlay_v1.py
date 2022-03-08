import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np


path = '/n/holyscratch01/cga/dkakkar/data/geotweets/results/2020' # use your path
all_files = glob.glob(path + "/*.gz")
l_gf_ni=[]

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=10897, dbname="omnisci") #use your port number
print("Connected",conn)

l_ni=[]
conn.execute("DROP TABLE IF EXISTS geotweets;")
conn.execute("Drop table if exists geotweets_fips;")
conn.execute("Create table IF NOT EXISTS geotweets_fips (message_id BIGINT,tweet_date TIMESTAMP,tweet_text TEXT,tweet_lang TEXT ENCODING DICT(32), retweets BIGINT,latitude DOUBLE,longitude DOUBLE,GPS TEXT ENCODING DICT(32),spatialerror DOUBLE,rowid0 BIGINT, FIPS text);")
for filename in all_files:
    print(filename)
    try:
      df = pd.read_csv(filename, sep='\t',dtype='unicode',index_col=None, low_memory='true',compression='gzip')
    except:
      l_ni.append(filename)
      continue

    df.drop(['geom', 'tags', 'source','place','tweet_favorites','photo_url','quoted_status_id','user_id','user_name','user_location','followers','friends','user_favorites','status','user_lang','data_source'],axis=1, inplace=True) 

    df.columns=['message_id','tweet_date','tweet_text','tweet_lang','retweets','latitude','longitude','GPS','spatialerror'] 
    cols = ['message_id', 'retweets', 'latitude', 'longitude', 'spatialerror']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df['tweet_date'] = pd.to_datetime(df['tweet_date'], errors='coerce')
    values = {'message_id':0, 'retweets':0,'latitude':0, 'longitude':0,'spatialerror':0}
    df.fillna(value=values, inplace = True)
    #print(df.head())
    try:
       conn.load_table("geotweets",df,create='infer',method='arrow')

    except:
       try:
         conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP,tweet_text TEXT,tweet_lang TEXT ENCODING DICT(32), retweets BIGINT,latitude DOUBLE,longitude DOUBLE,GPS TEXT ENCODING DICT(32),spatialerror DOUBLE);")
         conn.load_table_columnar("geotweets", df,preserve_index=False) 
       
       except:
         
         l_ni.append(filename)
         
         continue
         #conn.execute("Drop table if exists geotweets_fips")
    try:
       #print("Inside loading geotweets_fips", filename)
       conn.execute("INSERT INTO geotweets_fips (Select a.*,b.fips from geotweets a inner join omnisci_counties b ON ST_Intersects(b.omnisci_geo,ST_SetSRID(ST_Point(a.longitude,a.latitude),4326)));")
       #conn.execute("Drop table if exists geotweets;")
    except:
       print("Not loaded geotweets_fips")
       l_gf_ni.append(filename)
    conn.execute("Drop table if exists geotweets;")
#conn.execute("Select count(*) from geotweets_fips;") 
    #conn.execute("Drop table if exists geotweets;")
print(l_gf_ni)
print(l_ni)
