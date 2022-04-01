import glob
import pandas as pd
import os
import numpy as np

year = 2010

data_path = "/n/cga/data/geo-tweets/cga-sbg/{}".format(year)
# Read in hourly files
files = glob.glob(os.path.join(data_path, "*.csv.gz"))

df = pd.DataFrame()

for file in files:
    temp = pd.read_csv(file, compression='gzip', header=0, sep='\t', quotechar='"', error_bad_lines=False, lineterminator="\n")
    print(file)
    
    for i in range(len(temp['message_id'])):
        if temp['longitude'][i]=="True":
            temp['longitude'][i] = np.nan
        temp['date'][i]= temp['date'][i][0:10]
    #temp['in_usa'] = ((temp['latitude']>24) & (temp['latitude']<50) & (temp['longitude'].astype(float)>-125) & (temp['longitude'].astype(float)<-66))
    temp = temp.groupby('date')
   
    temp = pd.DataFrame({
        'nb_tweets': temp['message_id'].count(),
        #'nb_us_tweets': temp['in_usa'].sum()
    }).reset_index()
    
    df = pd.concat([df, temp])

# Go from Hourly to daily
df = df.groupby('date')

df = pd.DataFrame({
    'nb_tweets': df['nb_tweets'].sum(),
    #'nb_us_tweets': df['nb_us_tweets'].sum()
}).reset_index()

df.to_csv('/n/cga/data/geo-tweets/geotweet-sentiment-geography/total_tweets/results/tweet_counts_{}.csv'.format(year), index=False)
#df.groupby(df["date"]).sum().plot(kind="line")
