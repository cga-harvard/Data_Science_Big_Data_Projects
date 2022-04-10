%time
import pandas as pd
import os
from tqdm.notebook import tqdm

path = "/n/cga/data/geo-tweets/cga-sbg/2022/"
path2 = "/n/cga/data/geo-tweets/cga-sbg-sentiment/2022/"
out_path = "/n/holyscratch01/cga/sentiment_tweet/2022/"

already = [item.split(".")[0] for item in os.listdir(out_path)]
for file_name in tqdm(os.listdir(path)):
    # print(file_name)
    # if file_name.split(".csv")[0] in already:
    #     continue
    try:
        data = pd.read_csv(path+file_name, sep='\t',lineterminator='\n',dtype='unicode',index_col=None, 
                          compression='gzip')
        # print(data.dtypes)
    except Exception as e:
        print(e,path+file_name)
        continue
    # data_list = []
    # for k,v in tqdm(data.iterrows(),total =data.shape[0],leave=False ):
    #     message_id = v["message_id"]
    #     try:
    #         int(message_id)
    #         data_list +=[v]
    #     except Exception as e:
    #         print(e)
    #         pass
    # data = pd.DataFrame(data_list)
    data["message_id"] = data.message_id.astype(int)    
    data = data[['message_id', 'date', 'text', 'latitude', 'longitude']]
    data = data.set_index("message_id")
    try:
        temp = pd.read_csv(path2+"bert_sentiment_"+file_name,sep='\t',dtype='unicode',index_col=None, 
                           compression='gzip')
    except Exception as e:
        print(e,file_name)
        continue
        
    temp["message_id"] = temp["message_id"].astype(int) 
    temp = temp.set_index("message_id")
    # print(data.shape,temp.shape)
    data = data.merge(temp,left_index=True, right_index=True,how="left")
    # data = pd.merge(data,temp,on="message_id",how = "left")
    data.to_csv(out_path + file_name.split("csv")[0]+"csv" )
    # break
# data.head()
