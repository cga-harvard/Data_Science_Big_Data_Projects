# usage: python src/main_geography_imputer.py --port_number 9036

import pandas as pd
from pymapd import connect
import glob
import os
import pyarrow as pa
import numpy as np
import argparse

from utils.data_read_in import read_in

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', default="/n/holyscratch01/cga/nicogj/geo_input/", type=str, help='path to data')
    parser.add_argument('--output_path', default="/n/holyscratch01/cga/nicogj/geo_output/", type=str, help='path to save data')
    parser.add_argument('--port_number', default=7577, type=int, help='OmniSci port number')
    parser.add_argument('--batch_size', default=250, type=int, help='How many files to process at once')
    args = parser.parse_args()

    print("Connecting to Omnisci")
    conn=connect(user="admin", password="HyperInteractive", host="localhost", port=args.port_number, dbname="omnisci") #use your port number
    print("Connected",conn)

    conn.execute("DROP TABLE IF EXISTS adm2;")
    conn.execute("DROP TABLE IF EXISTS admin2;")
    conn.execute("COPY adm2 FROM '{}' WITH (geo='true');".format(os.path.join(os.getcwd(), "data/spatial/adm2.shp")))
    conn.execute("CREATE TABLE admin2 AS SELECT OBJECTID, ID_0, NAME_0, ISO, ID_1, NAME_1, ID_2, NAME_2, omnisci_geo FROM adm2;")
    conn.execute("DROP TABLE IF EXISTS adm2;")

    all_files = glob.glob(args.data_path + "*.gz")
    all_files = sorted([os.path.basename(elem) for elem in all_files])

    nb_batches = int(np.ceil(len(all_files)/args.batch_size))

    for i in range(nb_batches):

        file_batch = all_files[i*args.batch_size:(i+1)*args.batch_size]

        conn.execute("DROP TABLE IF EXISTS geo_adm2;")
        conn.execute("DROP TABLE IF EXISTS geotweets;")

        for file in file_batch:
            try:
                df = read_in(file, path=args.data_path, cols=['message_id', 'latitude', 'longitude'])
                # df = pd.read_csv(file, sep='\t',dtype='unicode',index_col=None, low_memory='true',compression='gzip')
            except:
                print("Corrupt file", file)
                continue

            cols = ['message_id', 'latitude', 'longitude']
            df = df[cols]
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            values = {'message_id':0, 'latitude':0, 'longitude':0}
            df.fillna(value=values, inplace = True)

            df['filename'] = file

            try:
               conn.load_table("geotweets", df, create='infer', method='arrow')
            except:
               try:
                 conn.execute("Create table IF NOT EXISTS geotweets (message_id BIGINT,latitud FLOAT, longitude FLOAT);")
                 print ("Inserted columnar", file)
               except:
                 print("Not inserted",file)
                 continue

        conn.execute("CREATE TABLE geo_adm2 AS (SELECT a.filename, a.message_id, b.OBJECTID, b.ID_0, b.NAME_0, b.ISO, b.ID_1, b.NAME_1, b.ID_2, b.NAME_2 FROM geotweets AS a,admin2 AS b WHERE ST_Intersects(b.omnisci_geo,ST_SetSRID(ST_Point(a.longitude, a.latitude), 4326)));")

        for file in file_batch:
            conn.execute("COPY (SELECT message_id, OBJECTID, ID_0, NAME_0, ISO, ID_1, NAME_1, ID_2, NAME_2 FROM geo_adm2 WHERE filename = '{}') TO '{}' with (delimiter = '\t', quoted = 'true', header='true');".format(
                file, os.path.join(args.output_path, "geography_{}".format(file.replace('.gz', '')))
            ))
