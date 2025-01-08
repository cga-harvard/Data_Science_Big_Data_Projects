import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=False)
import pandas as pd
from tqdm import tqdm

def spatial_join(row,blocks_gdf,block_suffix):

    df = pd.read_csv(row["input_file"],
                 sep = "\t", lineterminator="\n", dtype="unicode", index_col=None,  compression = "gzip",
                )
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(
            df["longitude"],df["latitude"]), crs = "EPSG:4326")
    join_inner_df = gdf.sjoin(blocks_gdf, how="inner")
    join_inner_df = join_inner_df.drop(["geom"], axis =1)
    join_inner_df.to_parquet(row["output_file"].replace(".parquet",f"-{block_suffix}.parquet"))


input_path_base = "/n/holylabs/LABS/cga/Lab/data/geo-tweets/cga-sbg-tweets"
output_path_base = "/n/netscratch/cga/Lab/xiaokang/tweets_us_census"
census_data_path = "/n/netscratch/cga/Lab/xiaokang/scripts/data/census_data"
#year = 2023

# read file paths
files_df = pd.DataFrame()
for year in range(2012, 2023):
    input_path = os.path.join(input_path_base, str(year))
    output_path = os.path.join(output_path_base, str(year))
    os.makedirs(output_path, exist_ok=True)
    input_file_list = [os.path.join(input_path, file) for file in os.listdir(input_path) if file.endswith(".csv.gz")]
    file_names = [file.split("/")[-1] for file in input_file_list]
    output_file_names = [os.path.join(output_path, file.split(".csv.gz")[0] + ".parquet") for file in file_names]
    files_df = pd.concat([files_df, pd.DataFrame({"input_file": input_file_list, "output_file": output_file_names,"file_name": file_names, "year": year})])

# process data
#files_df = files_df[files_df["year"] == year]
for census_file_name in tqdm(list(os.listdir(census_data_path))):
    suffix = census_file_name.split(".zip")[0]
    block = gpd.read_file(os.path.join(census_data_path,census_file_name)).to_crs("EPSG:4326")
    files_df.parallel_apply(spatial_join,args=(block,suffix,), axis=1)

print("all done!")