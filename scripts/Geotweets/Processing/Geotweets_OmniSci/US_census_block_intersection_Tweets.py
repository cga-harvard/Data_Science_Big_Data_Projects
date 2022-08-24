
import pandas as pd
import os
import geopandas as gpd
from tqdm.notebook import trange,tqdm
from pandarallel import pandarallel
import us


# df.apply(func)
def save_tweets(x):
    file_name = x.file_name

    s = us.states.lookup(file_name.split("_")[2])
    # print(s.abbr)
 
    cenus_shp = gpd.read_file(path + file_name)
    cenus_shp = cenus_shp.to_crs("EPSG:4326")
    
    for i in trange(1,13):
        out_filename = "2021_%d_%s.csv"%(i,s.abbr)
        if out_filename in alread_set:
            continue
        

        f = output_path + "2021_%d_%s.parquet"%(i,s.abbr)
        # print(f)
        
        # cenus_shp = gpd.read_file(path + file_name)
        # cenus_shp = cenus_shp.to_crs("EPSG:4326")
        try:
            df = pd.read_parquet(f)
        except Exception as e:
            print(e,f)
            continue
        df = gpd.GeoDataFrame(df, geometry= gpd.points_from_xy(df.longitude,df.latitude),crs="EPSG:4326")
        join_inner_df = df.sjoin(cenus_shp, how="inner")
        join_inner_df= join_inner_df[['message_id', 'tweet_date', 'tweet_text', 'tweet_lang', 'place',
               'user_id', 'user_name', 'user_location', 'followers', 'friends',
               'latitude', 'longitude', 'GPS', 'spatialerror', 'fips', 'county',
               'state',  'GEOID20']]
        join_inner_df.to_csv(outpath + out_filename,encoding = "utf-8-sig")
        
        # break
    return 1
    

# Divide the tweets to state-month files

input_path = "/n/holyscratch01/cga/xiaokang/gov_project/results/2021/"
output_path = '/n/holyscratch01/cga/xiaokang/gov_project/results/tweet_states/2021/parquet/'


temp = {}
for i in range(1,13):
    temp[i]=[]
    
for file_name in tqdm(os.listdir(input_path),total = len(os.listdir(input_path))):
    [y,m,d,h]=map(int,file_name.split(".")[0].split("_"))
    data = pd.read_csv(input_path+ file_name, delimiter = '\t', quotechar  = "\"",dtype={"fips":str,"GPS":str})
    data['tweet_date'] = pd.to_datetime(data['tweet_date'], errors='coerce',unit="s")
    temp[m] +=[data]
    # break

for k in tqdm(temp):
    data_list = temp[k]
    data_ = pd.concat(data_list)
    for i,item in data_.groupby("state"):
        item.to_parquet(output_path + "2021_%d_%s.parquet"%(k,i))
        # break

## Run spatial join

# import tqdm
pandarallel.initialize(progress_bar=True)
# tqdm.pandas()
path = "/n/holyscratch01/cga/xiaokang/Census_Blocks/"
outpath = '/n/holyscratch01/cga/xiaokang/gov_project/results/census/2021/'
output_path = '/n/holyscratch01/cga/xiaokang/gov_project/results/tweet_states/2021/parquet/'
alread_set =  set(os.listdir(outpath))
states_list = pd.Series(os.listdir(path),name="file_name").to_frame()

# states_list.progress_apply(save_tweets,axis=1)
temp=states_list.parallel_apply(save_tweets,axis=1)