# -*- coding: utf-8 -*-
import glob
import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

year = 2012

data_path = "/n/holylabs/LABS/cga/Lab/data/geo-tweets/cga-sbg/{}".format(year)
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
    
    temp['uk'] = ((temp['latitude']>49.959999905) & (temp['latitude']<58.6350001085) & (temp['longitude'].astype(float)>-7.57216793459) & (temp['longitude'].astype(float)<1.68153079591))
    temp['south_korea'] = ((temp['latitude']>34.3900458847) & (temp['latitude']<38.6122429469) & (temp['longitude'].astype(float)>126.117397903) & (temp['longitude'].astype(float)<129.468304478))
   
    temp = pd.DataFrame({
        'nb_uk_tweets': [temp['uk'].sum()],
        'nb_south_korea_tweets': [temp['south_korea'].sum()]
    })
    
    df = pd.concat([df, temp])


countUK=df['nb_uk_tweets'].sum()
countSK=df['nb_south_korea_tweets'].sum()

print(countUK)
print(countSK)
print("Finished！")
