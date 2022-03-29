import glob
import pandas as pd
import os

year = 2010

data_path = "/n/cga/data/geo-tweets/cga-sbg/{}".format(year)

# Read in hourly files
files = glob.glob(os.path.join(data_path, "*.csv.gz"))

df = pd.DataFrame()
for file in files:
    temp = pd.read_csv(file)
    #temp['in_usa'] = (temp['latitude']>24 & temp['latitude']<50 & temp['longitude']>-125 & temp['longitude']<-66)
    temp = temp.groupby('date')
    temp = pd.DataFrame({
        'nb_tweets': temp['tweet_id'].count(),
        #'nb_us_tweets': temp['in_usa'].sum()
    }).reset_index()

    df = pd.concat([df, temp])

# Go from Hourly to daily
df = df.groupby('date')
df = pd.DataFrame({
    'nb_tweets': df['nb_tweet'].sum(),
    #'nb_us_tweets': df['nb_us_tweets'].sum()
}).reset_index()

df.to_csv('/n/holyscratch01/cga/dkakkar/tweet_count/tweet_counts_{}.csv'.format(year), index=False)
