import os
import glob
import psycopg2
path = '/n/cga/geotweets_merged' # use your path
all_files = glob.glob(path + "/archive_backup_2019*")
for filename in all_files:
    print(filename)
    #archive_name="/n/cga/geotweets_merged/" + filename
    #print (archive_name)
    cmd= "pg_restore  --host localhost --port 7337 -U postgres -d postgres --section=pre-data --section=data -1 "+ filename
    #print(cmd)
    os.system(cmd)
    TBL= 'archive'+'.'+'twitter'+'_'+filename.split('_')[3]+'_'+filename.split('_')[4]
    #print(TBL)
    pwd='/n/holyscratch01/cga/dkakkar/data/geotweets/results/2019/'+'twitter'+'_'+filename.split('_')[3]+'_'+filename.split('_')[4]+'.csv'
    pwd= "'{}'".format(pwd)
    #print(pwd)
    try:
      conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' port='7337' password='1234'")
    except:
      print ("Unable to connect to the database")
    cur = conn.cursor()
    query="copy (Select message_id,date,text,tags,tweet_lang,source,place, ST_AsText(geom) as geom,retweets,tweet_favorites,photo_url,quoted_status_id,user_id,user_name,user_location,followers,friends,user_favorites,status,user_lang,latitude,longitude,data_source from " + str(TBL)+ ") "+ "to "+ str(pwd) + " DELIMITER '|' CSV HEADER;"
    #print(query)
    cur.execute(query)
#    cur.execute("""copy (Select message_id,date,text,tags,tweet_lang,source,place, ST_AsText(geom) as geom,retweets,tweet_favorites,photo_url,quoted_status_id,user_id,user_name,user_location,followers,friends,user_favorites,status,user_lang,latitude,longitude,data_source from archive.twitter_2010_10) to '/n/holyscratch01/cga/dkakkar/data/test.csv' DELIMITER '|' CSV HEADER;)
