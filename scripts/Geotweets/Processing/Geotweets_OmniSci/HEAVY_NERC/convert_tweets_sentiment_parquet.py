# -*- encoding: utf-8 -*-
import click
import pandas as pd
import os
from tqdm import tqdm
import logging



log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)

# config path here
tweets_path = "/data/geotweets/output/"
sentiment_path = "/data/geotweets/sentiment_output/2022/"
#output_path = "/data/kang/xiaokang/ur_war/data/interim/"
output_path = "/data/geotweets/sentiment_tweets_output/parquet_files/2022/"



"""Combine the geotweets with sentiment results"""
#print(output_path)
already = [item.split(".")[0] for item in os.listdir(output_path)]
#print(already)

for file_name in tqdm(os.listdir(tweets_path)):
    
    if file_name.split(".csv")[0] in already:
        continue
    #if "2023_1_01_00" not in file_name:
       # continue
    try:
        data = pd.read_csv(tweets_path+file_name, sep='\t',lineterminator='\n',dtype='unicode',index_col=None, 
                        compression='gzip')
    except Exception as e:
        # print(e,path+file_name)
        continue
    # 
    data["message_id"] = data.message_id.astype(int)  
    # break  
    # data = data[['message_id', 'date', 'text', 'latitude', 'longitude']]
    data = data.set_index("message_id")
    try: 
        df.drop(df.index, inplace=True)
        temp = pd.read_csv(sentiment_path+"bert_sentiment_"+file_name,sep='\t',dtype='unicode',index_col=None, 
                        compression='gzip')
    except Exception as e:
        # print(e,file_name)
        
        logger = logging.getLogger(__name__)
        logger.info(e,'file_name') 
        continue
    # print(file_name)
    temp["message_id"] = temp["message_id"].astype(int) 
    temp = temp.set_index("message_id")
    # break
    # print(data.shape,temp.shape)
    data = data.merge(temp,left_index=True, right_index=True,how="left")
    data = data.rename({"date_x":"tweet_date","user_id_x":"user_id",
     'text':"tweet_text","score":"sentiment_score"},axis=1)
    data = data.rename({'date':"tweet_date"},axis=1)

    data['tweet_date'] = pd.to_datetime(data['tweet_date'], errors='coerce')

    data = pd.merge(data,temp,on="message_id",how = "left")
    # csv files
    # data.to_csv(output_path + file_name.split("csv")[0]+"csv" )
    # parquet files
    # print(file_name)
    # break
    data.to_parquet(output_path + file_name.split("csv")[0]+"parquet",engine='pyarrow' )
        # 

