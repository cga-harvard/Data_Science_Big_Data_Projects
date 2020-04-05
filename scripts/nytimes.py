import os
import wget
import pandas as pd
import pyarrow as pa;import numpy as np
import pymapd
from pymapd import connect
#os.system("!wget https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv  -O us-counties.csv")
url=f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv'
url_1=f'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
wget.download(url)
wget.download(url_1)
df = pd.read_csv('us-states.csv', sep=',',dtype='unicode',index_col=None, low_memory='true')
df1 = pd.read_csv('us-counties.csv', sep=',',dtype='unicode',index_col=None, low_memory='true')
df['record_date']=df['date']
df['record_date']=pd.to_datetime(df['record_date'], errors='coerce')
df['cases']=pd.to_numeric(df['cases'], errors='coerce')
df['deaths']=pd.to_numeric(df['deaths'], errors='coerce')
df['fips']=pd.to_numeric(df['fips'], errors='coerce')
df.drop(['date'],axis=1,inplace=True)
#print(df.columns)
df1['record_date']=df1['date']
df1['record_date']=pd.to_datetime(df1['record_date'], errors='coerce')
df1['cases']=pd.to_numeric(df1['cases'], errors='coerce')
df1['deaths']=pd.to_numeric(df1['deaths'], errors='coerce')
df1['fips']=pd.to_numeric(df1['fips'], errors='coerce')
df1.drop(['date'],axis=1,inplace=True)
#conn.execute("DROP table if exists usstates")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9398, dbname="omnisci") #use your port number
conn.execute("DROP table if exists usstates")
conn.load_table("usstates",df,create='infer',method='arrow')
os.system("rm us-states.csv")
conn.execute("DROP table if exists uscounties")
conn.load_table("uscounties",df1,create='infer',method='arrow')
os.system("rm us-counties.csv")

