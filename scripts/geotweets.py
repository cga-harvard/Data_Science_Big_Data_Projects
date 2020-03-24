import pandas as pd
import numpy as np
from pathlib import Path
import shapely
from shapely import wkb
from geopandas import GeoDataFrame
import pyarrow
import math
from math import pi

def process(df): 
   
    df.date = pd.to_datetime(df.date)
    df['year'] = (df.date).dt.year
    df['month'] = (df.date).dt.month
    df['day'] = (df.date).dt.day
    df['hour'] = (df.date).dt.hour
    
    crs = {'init': 'epsg:4326'}
    geom= df['geom'].map(shapely.wkt.loads)
    gdf = GeoDataFrame(df, crs=crs, geometry=geom)
    #gdf['latitude']=pd.to_numeric(gdf['latitude'],errors='coerce')
    #gdf['longitude']=pd.to_numeric(gdf['longitude'],errors='coerce')
    #gdf['longitude']=gdf['longitude'].to_numeric()
  
    #gdf['GPS']= ~((gdf['latitude'].isnull()) | (gdf['longitude'].isnull()))
    
    #gdf = GeoDataFrame(df, crs=crs, geometry=geom)
    #gdf['centroidseries'] = gdf['geometry'].centroid
    gdf['GPS']= ((gdf['geometry'].geom_type)=='Point')
    #print((gdf['geometry'].geom_type), gdf['GPS']) 
    gdf['centroidseries'] = gdf['geometry'].centroid
    #x,y = [list(t) for t in zip(*map(getXY, centroidseries))]
    #print(x,y)
    gdf['longitude'] = gdf['centroidseries'].apply(lambda p: p.x)
    
    gdf['latitude'] = gdf['centroidseries'].apply(lambda p: p.y)
    gdf['area']= ((gdf['geometry']).to_crs("EPSG:3035")).area
    gdf['spatialerror']=((gdf['area']/pi)**(1/2))
   
    gdf['spatialerror'] = gdf['spatialerror'].replace(0, 10)
    
    year_month_day_hour = gdf[['year', 'month', 'day','hour']]
    year_month_day_hour = year_month_day_hour.drop_duplicates(subset=None, keep='first', inplace=False)
    year_month_day_hour = year_month_day_hour.reset_index()
    
    #print(gdf.head()) 
    #print(year_month_day_hour)
    
    for i in range(len(year_month_day_hour)):
        temp = gdf.loc[(gdf.year == year_month_day_hour.year[i]) & 
                      (gdf.month == year_month_day_hour.month[i]) & 
                      (gdf.day == year_month_day_hour.day[i]) &
                      (gdf.hour == year_month_day_hour.hour[i])]                
        
        year = str(year_month_day_hour.year[i])
        
        if (year_month_day_hour.year[i])< 10:
            month= ('0'+ str(year_month_day_hour.month[i]))
        else:
            month= str(year_month_day_hour.month[i])
        if (year_month_day_hour.day[i])< 10:
            day= ('0'+ str(year_month_day_hour.day[i]))
        else:
            day= str(year_month_day_hour.day[i])
        if (year_month_day_hour.hour[i])< 10:
            hour= ('0'+ str(year_month_day_hour.hour[i]))
        else:
            hour= str(year_month_day_hour.hour[i])
        
        filename = year+'_'+month+'_'+day+'_'+hour+'.csv'
        # filename = year+'_'+month+'_'+day+'_'+hour+'.parquet'

        # drop unnecessary columns
        temp.drop(['month','year','day','hour','geometry','centroidseries','area'],axis=1,inplace=True)
        #print(temp.columns())
   
        if Path(filename).is_file():
            # file exists alreadty, do not include header
            temp.to_csv(filename, mode = 'a', sep = '\t', header = False, index = False)
            # temp.to_parquet(filename,compression='snappy',engine='auto',index=None)
        else:
            # file doesn't exist, include header
            temp.to_csv(filename, sep = '\t', header = True, index = False)
            # temp.to_parquet(filename,compression='snappy',engine='auto',index=None)

chunksize = 1000000
for chunk in pd.read_csv('/n/holyscratch01/cga/dkakkar/data/geotweets/input/twitter_2020_02.csv', sep = '|', chunksize=chunksize): #Change file name and path here
    process(chunk)
