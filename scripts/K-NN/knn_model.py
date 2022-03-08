#Import all needed libraries

import pandas as pd
import os
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np

#Read all csv in the folder
#CSV are GZIP compressed

path = '/n/holyscratch01/enos_lab/dkakkar/partisan/data/' # use your path
all_files = glob.glob(path + "/*.gz")

## Create connection to Omnisci

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9568, dbname="omnisci") #use your port number
print("Connected",conn)

## Drop existing tables

conn.execute("DROP TABLE IF EXISTS temp;")
conn.execute("DROP table IF EXISTS results;")

## Do for all files in the folder

for filename in all_files:
     #print(filename)
     resultfile= (filename.split('/')[-1]).split('.')[0]
     #print(resultfile)
     #Read the CSV in pandas dataframe ; CSV should be comma delimited and GZIP compressed
     df = pd.read_csv(filename, sep=',',dtype='unicode',index_col=None, low_memory='true',compression='gzip')     
     #print(df.head())
     ## Indicate columns of CSV as colums of dataframe
     df.columns=['source_id','neigbor_id','dist','dpost','rpost']
     ## Since all dataframe colmuns are read as string so specify the numeric columns and convert them to number
     cols=['dist','dpost','rpost']
     df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
     #Load the dataframe to Omnisci as table with name "temp"
     conn.load_table("temp",df,create='infer',method='arrow')
     # Load dpost,rpost table
     # Join temp with dpost, rpost table
     
     ## Execute the KNN modelling query on the joined table; change the final table name from "temp" to your tablename
     conn.execute("Create table results as (SELECT source_id, AVG(dpost) as mean_d_post, AVG(rpost) as mean_r_post, SUM(dpost * 1/(1+dist))/SUM(1/(1+dist)) as wtd_d_post, SUM(rpost * 1/(1+dist))/SUM(1/(1+dist)) as wtd_r_post FROM temp GROUP BY source_id);")
     ## Copy the results table to CSV
     conn.execute("Copy (Select * from results) to '/n/holyscratch01/enos_lab/dkakkar/partisan/results.csv';")
     ##Rename the results CSV to input data CSV
     os.rename('results.csv',resultfile+'.csv')
     #os.gzip(resultfile+'.csv')
