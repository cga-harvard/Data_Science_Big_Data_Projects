import glob
import pandas as pd
import os
import numpy as np
import math
import matplotlib.pyplot as plt

year = 2010

data_path = "/n/cga/data/geo-tweets/cga-sbg/{}".format(year)
# Read in hourly files
files = glob.glob(os.path.join(data_path, "*.csv.gz"))

df = pd.DataFrame()

for file in files:
    temp = pd.read_csv(file, compression='gzip', header=0, sep='\t', quotechar='"', error_bad_lines=False, lineterminator="\n")
    #print(file)
    
    temp['date'] = pd.to_datetime(temp['date']).dt.date
    
    for i in range(len(temp['message_id'])):
        if temp['longitude'][i]=="True":
            temp['longitude'][i] = np.nan
    #For finding tweets in US        
    #temp['in_usa'] = ((temp['latitude']>24) & (temp['latitude']<50) & (temp['longitude'].astype(float)>-125) & (temp['longitude'].astype(float)<-66))
    temp = temp.groupby('date')
   
    temp = pd.DataFrame({
        'nb_tweets': temp['message_id'].count(),
        #'nb_us_tweets': temp['in_usa'].sum()
    }).reset_index()
    
    df = pd.concat([df, temp])

# Go from Hourly to daily
df = df.groupby('date')

#print(df.dtypes)

df = pd.DataFrame({
    'nb_tweets': df['nb_tweets'].sum(),
    #Sum of US tweets
    #'nb_us_tweets': df['nb_us_tweets'].sum()
}).reset_index()

df.to_csv('tweet_counts_{}.csv'.format(year), index=False)


fig, ax = plt.subplots()
plt.ylabel("Total Tweets")
plt.xlabel("Date")
plt.title("Tweets Count over Time")

yint = range(min(df['nb_tweets']), math.ceil(max(df['nb_tweets']))+1)

plt.yticks(yint)

ax.plot(df['date'], df['nb_tweets'])


fig.autofmt_xdate()


plt.show()

plt.savefig('nb_tweets_over_time_{}.png'.format(year),bbox_inches='tight')



# Number of US twee#ts

#plt.plot(df['date'], df['nb_us_tweets'])

#plt.savefig('nb_us_tweets_over_time_{}.png'.format(year), bbox_inches='tight')


#print("Finished！")
