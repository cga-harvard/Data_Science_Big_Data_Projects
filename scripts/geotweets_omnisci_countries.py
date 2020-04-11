import pandas as pd
from pymapd import connect
from os import listdir
import glob
from os.path import isfile, join
import pyarrow as pa;import numpy as np
con = connect(user="admin", password= "HyperInteractive", host="localhost",port="8651", dbname="omnisci")
c = con.cursor()
c.execute("Create table usa as (SELECT * from omnisci_countries where sovereignt='United States of America');")
c.execute("Create table geotweets_us AS (Select a.message_id, a.longitude,a.latitude from geotweets a, usa b WHERE ST_Intersects(b.omnisci_geo,ST_SetSRID(ST_Point(a.longitude,a.latitude),4326)));")
