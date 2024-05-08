import pandas as pd
from heavyai import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np

path = '/n/holyscratch01/enos_lab/sunyoungp/data/raw/2014_789' # use your path
all_files = glob.glob(path + "/2014_8_*.gz")

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=10703, dbname="heavyai") #use your port number
print("Connected",conn)

l_ni=[]
conn.execute("DROP TABLE IF EXISTS geotweets;")
conn.execute("DROP TABLE IF EXISTS geotweets_kr;")
conn.execute("DROP TABLE IF EXISTS geotweets_fr;")
conn.execute("DROP TABLE IF EXISTS geotweets_nl;")

for filename in all_files:
    print(filename)
    try:
        df = pd.read_csv(filename, sep='\t',dtype='unicode',index_col=None, low_memory=True,compression='gzip')

        df = df.drop(['geom'], axis = 'columns')
        df.columns=['message_id','date','text','tags','tweet_lang','source','place','retweets','tweet_favorite', 'photo_url','quoted_status_id','user_id','user_name','user_location','followers','friends','user_favorites','status','user_lang','latitude','longitude','data_source','GPS','spatialerror']

        cols = ['message_id', 'retweets', 'tweet_favorite', 'quoted_status_id', 'user_id',
                'followers','friends', 'user_favorites','status',
                'latitude', 'longitude', 'spatialerror']
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        values = {'message_id':0, 'retweets':0, 'tweet_favorite':0, 'quoted_status_id':0, 'user_id':0,
                   'followers':0,'friends':0, 'user_favorites':0,'status':0,'latitude':0, 'longitude':0,
                   'spatialerror':0}
        df.fillna(value=values, inplace = True)

        string_cols = ['text', 'tweet_lang', 'place', 'user_name', 'user_location', 'user_lang', 'GPS']
        df[string_cols] = df[string_cols].astype(str)
        
        df.drop(['tags', 'source', 'retweets', 'tweet_favorite', 'photo_url', 'quoted_status_id', 'followers', 'friends', 'user_favorites', 'status', 'data_source'], axis=1, inplace=True)
        #df.drop(['tweet_lang', 'user_lang', 'user_location'], axis=1, inplace=True)
        df.rename(columns={'text': 'tweet_text', 'date': 'tweet_date'}, inplace=True)

        conn.load_table("geotweets",df,create='infer',method='arrow')
        print("Inserted Arrow 1", filename)

    except:
        try:
            df = pd.read_csv(filename, sep='\t', dtype='unicode', index_col=None, low_memory=True, compression='gzip', lineterminator='\n')
            
            df = df.drop(['geom'], axis = 'columns')
            df.columns=['message_id','date','text','tags','tweet_lang','source','place','retweets','tweet_favorite', 'photo_url','quoted_status_id','user_id','user_name','user_location','followers','friends','user_favorites','status','user_lang','latitude','longitude','data_source','GPS','spatialerror']

            cols = ['message_id', 'retweets', 'tweet_favorite', 'quoted_status_id', 'user_id',
                    'followers', 'friends', 'user_favorites', 'status',
                    'latitude', 'longitude', 'spatialerror']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

            # Fill NaN values
            values = {'message_id':0, 'retweets':0, 'tweet_favorite':0, 'quoted_status_id':0, 'user_id':0,
                    'followers':0,'friends':0, 'user_favorites':0,'status':0,'latitude':0, 'longitude':0,
                    'spatialerror':0}
            df.fillna(value=values, inplace = True) 

            string_cols = ['text', 'tweet_lang', 'place', 'user_name', 'user_location', 'user_lang', 'GPS']
            df[string_cols] = df[string_cols].astype(str)

            df.drop(['tags', 'source', 'retweets', 'tweet_favorite', 'photo_url', 'quoted_status_id', 'followers', 'friends', 'user_favorites', 'status', 'data_source'], axis=1, inplace=True)
            #df.drop(['tweet_lang', 'user_lang', 'user_location'], axis=1, inplace=True) 
            df.rename(columns={'text': 'tweet_text', 'date': 'tweet_date'}, inplace=True)

            # Try inserting data into the database
            conn.load_table("geotweets", df, create='infer', method='arrow')
            print("Inserted Arrow 2", filename)

        except:
            try:
                conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,tweet_date TIMESTAMP(0),tweet_text TEXT ENCODING NONE,tweet_lang TEXT ENCODING DICT(32),place TEXT ENCODING NONE,user_id BIGINT,user_name TEXT ENCODING NONE,user_location TEXT ENCODING NONE,user_lang TEXT ENCODING DICT(32),latitude FLOAT,longitude FLOAT,GPS TEXT ENCODING DICT(32),spatialerror FLOAT);")
                conn.load_table_columnar("geotweets", df, preserve_index=False) 
                print ("Inserted columnar", filename) 
                
            except Exception as e:
                l_ni.append(filename)
                print("Unexpected error occurred:", str(e))
                continue

print(l_ni)
