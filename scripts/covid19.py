import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np

#path = '/n/cga/blewis/data/gdelt/' # use your path
#all_files = glob.glob(path + "/*.CSV")

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9398, dbname="omnisci") #use your port number
print("Connected",conn)
conn.execute("DROP TABLE IF EXISTS covid19")
#conn.execute("Create table IF NOT EXISTS covid19 (province_state TEXT ENCODING DICT(32),country_region TEXT ENCODING DICT(32), lat FLOAT, lon FLOAT, dt TIMESTAMP(0),confirmed BIGINT, deaths BIGINT, recovered BIGINT, confirmed_diff BIGINT, deaths_diff BIGINT, recovered_diff BIGINT);")
#conn.execute("Create table IF NOT EXISTS covid19 (message_id BIGINT,tweet_date TIMESTAMP(0),tweet_text TEXT ENCODING NONE,tags TEXT ENCODING DICT(32),tweet_lang TEXT ENCODING DICT(32),source TEXT ENCODING DICT(32),place TEXT ENCODING NONE, retweets SMALLINT, tweet_favorites SMALLINT,photo_url TEXT ENCODING DICT(32),quoted_status_id BIGINT,user_id BIGINT,user_name TEXT ENCODING NONE,user_location TEXT ENCODING NONE,followers SMALLINT,friends SMALLINT,user_favorites INT,status INT,user_lang TEXT ENCODING DICT(32),latitude FLOAT,longitude FLOAT,data_source TEXT ENCODING DICT(32),GPS TEXT ENCODING DICT(32),spatialerror FLOAT);")
#print("copying")
#df['province_state'].fillna('NA', inplace=True)
#df['country_region'].fillna('NA', inplace=True)
#conn.execute("Copy covid19 FROM '/n/holyscratch01/cga/dkakkar/data/covid_19_clean_complete-20200325-212001.csv' WITH (nulls = 'NA')")

df = pd.read_csv('/n/holyscratch01/cga/dkakkar/data/covid19_omnisci/covid_19_clean_complete-20200325-212001.csv', sep=',',dtype='unicode',index_col=None, low_memory='true')
df['lat']=pd.to_numeric(df['lat'], errors='coerce')
df['lon']=pd.to_numeric(df['lon'], errors='coerce')
df['confirmed']=pd.to_numeric(df['confirmed'], errors='coerce')
df['deaths']=pd.to_numeric(df['deaths'], errors='coerce')
df['recovered']=pd.to_numeric(df['recovered'], errors='coerce')
df['deaths_diff']=pd.to_numeric(df['deaths_diff'], errors='coerce')
df['confirmed_diff']=pd.to_numeric(df['confirmed_diff'], errors='coerce')
df['recovered_diff']=pd.to_numeric(df['recovered_diff'], errors='coerce')
df['dt']=pd.to_datetime(df['dt'], errors='coerce')
df['province_state'].fillna('NA', inplace=True)
df['country_region'].fillna('NA', inplace=True)
#df['MonthYear']=pd.to_numeric(df['MonthYear'], errors='coerce')
#df['MonthYear']=pd.to_numeric(df['MonthYear'], errors='coerce')

#df = df.astype({  'lat':'float32',\
#                                'lon':'float32',\
#                                'confirmed':'int32',\
#                                'deaths':'int32',\
#                                'recovered':'int32',\
#                                'recovered_diff':'int32',\
#                                'deaths_diff':'int32',\
#                                'confirmed_diff':'int32'})
print(df.head())
conn.load_table("covid19",df,create='infer',method='arrow')
#conn.execute("Create table IF NOT EXISTS covid19 (message_id BIGINT,tweet_date TIMESTAM
