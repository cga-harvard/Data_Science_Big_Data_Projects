import pandas as pd
import numpy as np
from pathlib import Path
import shapely
from shapely import wkb
from geopandas import GeoDataFrame


def process(df): 
    
    
    crs = {'init': 'epsg:4326'}
    geom= df['geom'].map(shapely.wkt.loads)
    
    gdf = GeoDataFrame(df, crs=crs, geometry=geom)
    #print(gdf.head())
    gdf.date = pd.to_datetime(gdf.date)

    
    gdf['year'] = (gdf.date).dt.year
    gdf['month'] = (gdf.date).dt.month
    gdf['day'] = (gdf.date).dt.day
    gdf['hour'] = (gdf.date).dt.hour
    gdf['GPS']= ~(gdf['latitude'].isnull() | gdf['longitude'].isnull())
    

    gdf['centroidseries'] = gdf['geometry'].centroid
    gdf['longitude'] = gdf['centroidseries'].apply(lambda p: p.x)
    
    gdf['latitude'] = gdf['centroidseries'].apply(lambda p: p.y)
    
    
    year_month_day_hour = gdf[['year', 'month', 'day','hour']]
    year_month_day_hour = year_month_day_hour.drop_duplicates(subset=None, keep='first', inplace=False)
    year_month_day_hour = year_month_day_hour.reset_index()
                   
    gdf.drop(['month','year','day','hour','geometry','centroidseries'],axis=1,inplace=True)
    
    #print(gdf.head())
    
    
    for i in range(len(year_month_day_hour)):
        temp=gdf
        year= str(year_month_day_hour.year[i])
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
        
        filename = year+'_'+month+'_'+day+'_'+hour+'.csv'+'.gz'
        if Path(filename).is_file():
            # file exists alreadty, do not include header
            temp.to_csv(filename, mode = 'a', sep = '\t', header = False, index = False, compression='gzip')
            #temp.to_parquet(filename,compression='snappy',engine='auto',index=None)
        else:
            # file doesn't exist, include header
            temp.to_csv(filename, mode = 'a', sep = '\t', header = True, index = False, compression='gzip')
            

chunksize = 10000000
for chunk in pd.read_csv('/n/holyscratch01/cga/dkakkar/data/twitter_2019_12.csv', sep = '|', chunksize=chunksize): #Change file name and path here
    process(chunk)
