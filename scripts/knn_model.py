import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np

path = '/n/holyscratch01/enos_lab/dkakkar/partisan/data/' # use your path
all_files = glob.glob(path + "/*.gz")


print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9467, dbname="omnisci") #use your port number
print("Connected",conn)

conn.execute("DROP TABLE IF EXISTS temp;")
conn.execute("DROP table IF EXISTS results;")

for filename in all_files:
     print(filename)
     #conn.execute("Create table temp (source_id TEXT ENCODING DICT, neigbor_id TEXT ENCODING DICT,dist float, dpost float, rpost float);")
     df = pd.read_csv(filename, sep=',',dtype='unicode',index_col=None, low_memory='true',compression='gzip')     
     #print(df.head())
     df.columns=['source_id','neigbor_id','dist','dpost','rpost']
     cols=['dist','dpost','rpost']
     df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
     #conn.load_table_columnar("temp", df,preserve_index=False)
     conn.load_table("temp",df,create='infer',method='arrow')
     conn.execute("Create table results as (SELECT source_id, AVG(dpost) as mean_d_post, AVG(rpost) as mean_r_post, SUM(dpost * 1/(1+dist))/SUM(1/(1+dist)) as wtd_d_post, SUM(rpost * 1/(1+dist))/SUM(1/(1+dist)) as wtd_r_post FROM temp GROUP BY source_id);")
     conn.execute("Copy (Select * from results) to '/n/holyscratch01/enos_lab/dkakkar/partisan/$filename.csv';")
