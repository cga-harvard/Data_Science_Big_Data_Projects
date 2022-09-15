import os
import glob
import psycopg2
path = '/data/geotweets/daily/' # use your path
# tweets__ -> tweets_: 2021-Tao
all_files = glob.glob(path + "/tweets_*")
#os.system("module load postgis")
#os.system("module load Anaconda3/5.0.1-fasrc02")
#os.system("source activate omnisci")
for filename in all_files:
    print(filename)
    #archive_name="/n/cga/geotweets_merged/" + filename
    #print (archive_name)
    cmd= "pg_restore --host localhost --port 5432 -U postgres -d postgres --section=pre-data --section=data -1 "+ filename
    #print(cmd)
    os.system(cmd)
    #TBL= 'crawler'+'.'+'twitter'+'_'+filename.split('_')[3]+'_'+filename.split('_')[4]+filename.split('_')[5]
    #filename=(filename.split('/')[-1]).replace('__','_')
    filename_final=((filename.split('/')[-1]).replace('__','_')).replace('tweets_','')
    #print("final_filename",filename_final)
    TBL= 'crawler'+'.'+'twitter_crawler_daily_'+(filename_final)
    #print("printing table name",TBL)
    #pwd='/n/holyscratch01/cga/dkakkar/data/geotweets_merged_daily/input/'+(filename.split('/')[-1]).replace('__','_')+'.csv'
    pwd='/data/geotweets/input/'+(filename.split('/')[-1]).replace('__','_')+'.csv'
    pwd= "'{}'".format(pwd)
    #print("printing path",pwd)
    try:
      conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' port='5432' password='1234'")
    except:
      print ("Unable to connect to the database")
    cur = conn.cursor()
    query="Copy (Select message_id,date,text,tags,tweet_lang,source,place, ST_AsText(geom) as geom,retweets,tweet_favorites,photo_url,quoted_status_id,user_id,user_name,user_location,followers,friends,user_favorites,status,user_lang,latitude,longitude,data_source from " + str(TBL)+ ") "+ "to "+ str(pwd) + " DELIMITER '|' CSV HEADER;"
    print(query)
    cur.execute(query)
    query="Drop table "+ str(TBL) +";"
    print(query)
    cur.execute(query)
    query="Commit";
    cur.execute(query)
#    cur.execute("""copy (Select message_id,date,text,tags,tweet_lang,source,place, ST_AsText(geom) as geom,retweets,tweet_favorites,photo_url,quoted_status_id,user_id,user_name,user_location,followers,friends,user_favorites,status,user_lang,latitude,longitude,data_source from archive.twitter_2010_10) to '/n/holyscratch01/cga/dkakkar/data/test.csv' DELIMITER '|' CSV HEADER;)
