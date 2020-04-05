import os
import glob
path = '/n/cga/geotweets_merged' # use your path
all_files = glob.glob(path + "/archive_backup_2007*")
for filename in all_files:
    print(filename)
    #archive_name="/n/cga/geotweets_merged/" + filename
    #print (archive_name)
    cmd= "pg_restore  --host localhost --port 7337 -U postgres -d postgres --section=pre-data --section=data -1 "+ filename
    print(cmd)
    os.system(cmd)

