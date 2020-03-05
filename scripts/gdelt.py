import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np

path = '/n/holyscratch01/cga/dkakkar/data/Gdelt/' # use your path
all_files = glob.glob(path + "/*.CSV")

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=8604, dbname="omnisci") #use your port number
print("Connected",conn)
query="DROP TABLE IF EXISTS gdelt"
conn.execute(query)

for filename in all_files:
    #print(filename)
    df = pd.read_csv(filename, sep='\t',dtype='unicode',index_col=None, low_memory='true')
    df.columns=['GLOBALEVENTID','SQLDATE','MonthYear','Years','FractionDate','Actor1Code','Actor1Name','Actor1CountryCode','Actor1KnownGroupCode','Actor1EthnicCode','Actor1Religion1Code','Actor1Religion2Code','Actor1Type1Code','Actor1Type2Code','Actor1Type3Code','Actor2Code','Actor2Name','Actor2CountryCode','Actor2KnownGroupCode','Actor2EthnicCode','Actor2Religion1Code','Actor2Religion2Code','Actor2Type1Code','Actor2Type2Code','Actor2Type3Code','IsRootEvent','EventCode','EventBaseCode','EventRootCode','QuadClass','GoldsteinScale','NumMentions','NumSources','NumArticles','AvgTone','Actor1Geo_Type','Actor1Geo_FullName','Actor1Geo_CountryCode','Actor1Geo_ADM1Code','Actor1Geo_Lat','Actor1Geo_Long','Actor1Geo_FeatureID','Actor2Geo_Type','Actor2Geo_FullName','Actor2Geo_CountryCode','Actor2Geo_ADM1Code','Actor2Geo_Lat','Actor2Geo_Long','Actor2Geo_FeatureID','ActionGeo_Type','ActionGeo_FullName','ActionGeo_CountryCode','ActionGeo_ADM1Code','ActionGeo_Lat','ActionGeo_Long','ActionGeo_FeatureID','DATEADDED','SOURCEURL']
    #print(df.head(5))
    #li.append(df)

#frame = pd.concat(li, axis=0, ignore_index=True)
    df['SQLDATE']= pd.to_datetime(df['SQLDATE'])
    df['MonthYear']=pd.to_numeric(df['MonthYear'])
    df['Years']=pd.to_numeric(df['Years'])
    df['ActionGeo_Lat']=pd.to_numeric(df['ActionGeo_Lat'])
    df['ActionGeo_Long']=pd.to_numeric(df['ActionGeo_Long'])
    df['FractionDate']=pd.to_numeric(df['FractionDate'])
    df['IsRootEvent']=pd.to_numeric(df['IsRootEvent'])
    #df['EventCode']=pd.to_numeric(df['EventCode'])
    #df['EventBaseCode']=pd.to_numeric(df['EventBaseCode'])
    #df['EventRootCode']=pd.to_numeric(df['EventRootCode'])
    df['QuadClass']=pd.to_numeric(df['QuadClass'])
    df['NumMentions']=pd.to_numeric(df['NumMentions'])
    df['NumSources']=pd.to_numeric(df['NumSources'])
    df['NumArticles']=pd.to_numeric(df['NumArticles'])
    df['AvgTone']=pd.to_numeric(df['AvgTone'])
    df['Actor1Geo_Type']=pd.to_numeric(df['Actor1Geo_Type'])
    df['Actor1Geo_Lat']=pd.to_numeric(df['Actor1Geo_Lat'])
    df['Actor1Geo_Long']=pd.to_numeric(df['Actor1Geo_Long'])
    df['Actor2Geo_Lat']=pd.to_numeric(df['Actor2Geo_Lat'])
    df['Actor2Geo_Long']=pd.to_numeric(df['Actor2Geo_Long'])
    df['ActionGeo_Type']=pd.to_numeric(df['ActionGeo_Type'])
    df['ActionGeo_Lat']=pd.to_numeric(df['ActionGeo_Lat'])
    df['ActionGeo_Long']=pd.to_numeric(df['ActionGeo_Long'])
    df['DATEADDED']=pd.to_datetime(df['DATEADDED'])
    #print(df.head(5))
#frame.to_parquet('/n/holyscratch01/cga/dkakkar/data/gdelt.parquet.gzip',compression='gzip')
    conn.load_table("gdelt",df,create='infer',method='arrow')
    print("Inserted", filename)
