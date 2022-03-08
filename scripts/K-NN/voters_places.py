import pandas as pd
from pymapd import connect
#from os import listdir
#import glob
#from os.path import isfile, join
#import pyarrow as pa;import numpy as np


#path = '/n/holyscratch01/cga/dkakkar/data/geotweets/2020/' # use your path
#all_files = glob.glob(path + "/*.gz")

print("Connecting to Omnisci")
conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9549, dbname="omnisci") #use your port number
print("Connected",conn)
#query="DROP TABLE IF EXISTS geotweets"
#coinn.execute(query)
#states = ["PA"]
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
for state in states:
    print(state)
    #query="\clear_gpu_memory"
    #conn.execute(query)
    query="INSERT INTO us_voters_places (Select a.id,a.state,b.STATEFP10, b.PLACEFP10,b.NAME10 from partisan a inner join census_places_shapefiles b ON ST_Intersects(ST_SetSRID(b.omnisci_geo,4326),ST_SetSRID(ST_Point(a.lon,a.lat),4326)) where (a.state="+("'%s'" % state)+"));"
    #print(query)
    conn.execute(query)

#query="Copy (Select * from us_voters_places) to '/n/holyscratch01/cga/dkakkar/data/us_voters_places.csv';"
#conn.execute(query)
