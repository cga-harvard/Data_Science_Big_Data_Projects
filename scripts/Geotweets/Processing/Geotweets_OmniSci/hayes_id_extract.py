########################################################################
#  Date last modified: 08/28/2023                                      #
#  Author: Jack Hayes                                                  #
#  Author Email: jehayes@wm.edu                                        #
#  Script Objective: Extract and transfer message ID's from tweets     #
#                    for Harvard's Twitter Sentiment Global Index      #
########################################################################

# imports
import pandas as pd
import glob
from IPython.display import clear_output

# define input and output path and years of tweets to iterate through
input_path = "/n/holylabs/LABS/cga/Lab/data/geo-tweets/cga-sbg-sentiment/"
years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", 
         "2019", "2020", "2021", "2022", "2023"]
output_path = "/n/holyscratch01/cga/jehayes/"

# go through each year
for year in years:
    print("Compiling year...",year)
    # define new input and output paths for every year
    in_path = input_path + year
    out_path = output_path + year
    # for each file in the given year
    for i, file in enumerate(glob.glob(in_path + "/*.csv.gz")):
        # read in each file, only using the message_id field
        data = pd.read_csv(file, sep="\t", usecols=["message_id"])
        # write csv to output path while slightly changing the namiing convention
        data.to_csv(out_path + "/" + file.split("/")[-1].replace('.gz', '').replace('bert_sentiment', 'tweet_ids'), index=0)