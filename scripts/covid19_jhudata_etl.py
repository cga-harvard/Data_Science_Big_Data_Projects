import pandas as pd
import wget
import prefect
from prefect import task, Flow
from os import path
from pymapd import connect
from typing import Dict
from datetime import timedelta
from prefect.schedules import IntervalSchedule
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from prefect.schedules import CronSchedule

from datetime import datetime
import sys
import os
import logging

#TODO parametrize these in a conf file. Ideally we'd have one extractor class that takes a source, a dest table/db and produces a dict of table/df rather tha repeat this code
DATASOURCE_JHU_GITHUB = {   "confirmed":'time_series_covid19_confirmed_global.csv',\
                            "deaths":'time_series_covid19_deaths_global.csv',\
                            "recovered":'time_series_covid19_recovered_global.csv'}

#@task
def extract_gh_global_covid_ts_data() -> Dict[str, pd.DataFrame]:
    """Extract the 3 time series files and load into pandas dataframes for transformation"""
    #print("Inside logger")
    DATASOURCE_JHU_GITHUB = {   "confirmed":'time_series_covid19_confirmed_global.csv',\
                            "deaths":'time_series_covid19_deaths_global.csv',\
                            "recovered":'time_series_covid19_recovered_global.csv'}

    #logger = prefect.context.get("logger")

    dfs={}

    #TODO add date-based file download and caching logic
    try:
        for fn in DATASOURCE_JHU_GITHUB:
            #print("File name", fn)
            url = f'https://github.com//CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series/{DATASOURCE_JHU_GITHUB[fn]}'
            print("URL is", url)
            wget.download(url)
            print("Download comple", DATASOURCE_JHU_GITHUB[fn])
            #logging.info(f"Downloaded of:{url} successful")
            dfs[fn] = pd.read_csv(DATASOURCE_JHU_GITHUB[fn],error_bad_lines=False)
    except:
        logging.error(f'Failed to download time series files - exception occurred: {sys.exc_info()[0]}')
        raise

    return dfs

#@task
def transform_daily_covid_data(daily_report_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Consolidate the 3 time series files into one, and compute daily increments as well as filter out empty rows"""
    #logger = prefect.context.get("logger")

    conf_df = daily_report_dfs['confirmed']
    deaths_df = daily_report_dfs['deaths']
    recv_df = daily_report_dfs['recovered']

    #date columns start at column 5, pivot into long form
    dates = conf_df.columns[4:]

    #melting the dfs to turn them into a row per date
    conf_df_long = conf_df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], value_vars=dates, var_name='Date', value_name='Confirmed')
    deaths_df_long = deaths_df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], value_vars=dates, var_name='Date', value_name='Deaths')
    recv_df_long = recv_df.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], value_vars=dates, var_name='Date', value_name='Recovered')

    #munge these melted dfs together
    full_table = pd.concat([conf_df_long, deaths_df_long['Deaths'], recv_df_long['Recovered']], axis=1, sort=False)
    
    #subtle bug - df.shift doesnt work if you groupby on nulls in the province field, so have to use the lat/long and country fields to group by
    prev_day = full_table.groupby(['Country/Region', 'Lat', 'Long']).shift(1)
    confirmed_diff = full_table['Confirmed'].sub(prev_day['Confirmed'],fill_value=0).rename('confirmed_diff')

    deaths_diff = full_table['Deaths'].sub(prev_day['Deaths'],fill_value=0).rename('deaths_diff')
    recovered_diff = full_table['Recovered'].sub(prev_day['Recovered'],fill_value=0).rename('recovered_diff')

    full_table_diff = pd.concat([full_table, confirmed_diff, deaths_diff,recovered_diff], axis=1, sort=False)
    full_table_diff.fillna(0)
    full_table_diff.rename(columns={"Province/State": "province_state",\
                                    "Country/Region": "country_region",\
                                    "Lat": "lat",\
                                    "Long":"lon",\
                                    "Date":"dt",\
                                    "Confirmed":"confirmed",\
                                    "Deaths":"deaths",\
                                    "Recovered":"recovered"}, inplace=True)

    #select only rows with non-zero counts
    filt_df = full_table_diff[full_table_diff.iloc[:,5:].sum(axis=1) != 0]

    #Basic data cleaning steps because of garbage in the country field
    filt_df.loc[filt_df['country_region'] == 'US','country_region'] = 'United States'
    filt_df.loc[filt_df['country_region'] == 'Korea, South','country_region'] = 'Korea'
    filt_df.loc[filt_df['country_region'] == 'Taiwan*','country_region'] = 'Taiwan'
    filt_df.loc[filt_df['country_region'] == 'Russia','country_region'] = 'Russian Federation'
    filt_df.loc[filt_df['country_region'] == 'Czechia','country_region'] = 'Czech Republic'
    filt_df.loc[filt_df['country_region'] == 'Brunei','country_region'] = 'Brunei Darussalam'
    filt_df.loc[filt_df['country_region'] == 'North Macedonia','country_region'] = 'Macedonia'
    filt_df.loc[filt_df['country_region'] == 'Congo (Kinshasa)','country_region'] = 'Democratic Republic of the Congo'
    filt_df.loc[filt_df['country_region'] == 'Congo (Brazzaville)','country_region'] = 'Republic of the Congo'
    filt_df.loc[filt_df['country_region'] == "Cote d'Ivoire",'country_region'] = "Côte d'Ivoire"
    filt_df.loc[filt_df['country_region'] == 'Eswatini','country_region'] = 'eSwatini'
    filt_df.loc[filt_df['country_region'] == 'Bahamas, The','country_region'] = 'Bahamas'
    filt_df.loc[filt_df['country_region'] == 'Gambia, The','country_region'] = 'The Gambia'
    filt_df.loc[filt_df['country_region'] == 'Cabo Verde','country_region'] = 'Republic of Cabo Verde'
    filt_df.loc[filt_df['country_region'] == 'U.S. Virgin Islands','country_region'] = 'United States Virgin Islands'
    filt_df.loc[filt_df['country_region'] == 'Holy See','country_region'] = 'Vatican'
    filt_df.loc[filt_df['country_region'] == 'Martinique','country_region'] = 'France'

    #TODO - fix the copy warning here
    dateify_func = lambda dt_str: datetime.strptime(dt_str, '%m/%d/%y')
    filt_df['dt'] = filt_df['dt'].apply(dateify_func)

    #write this out
    logging.info(f"Transformed report files. Record count:{len(filt_df.index)}. Writing to clean file")

    filt_df = filt_df.astype({  'lat':'float32',\
                                'lon':'float32',\
                                'confirmed':'int32',\
                                'deaths':'int32',\
                                'recovered':'int32',\
                                'recovered_diff':'int32',\
                                'deaths_diff':'int32',\
                                'confirmed_diff':'int32'})
    #print(filt_df.head())
    #Load to Omnisci
    print("Connecting to Omnisci")
    conn=connect(user="admin", password="HyperInteractive", host="localhost", port=9398, dbname="omnisci") #use your port number
    print("Connected",conn)
    conn.execute("DROP TABLE IF EXISTS covid19")
    print(df.head())
    conn.load_table("covid19",df,create='infer',method='arrow')
    #TODO parametrize this
    clean_file_name=f'covid_19_clean_complete-{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv'
    filt_df.to_csv(clean_file_name, index=False)

    return {'table_name':'daily_covid', 'table_data':filt_df}

#@task
def cleanup_files():
    """Remove the source files to avoid cluttering the run directory"""
    #print("Inside cleanup")
    #logger = prefect.context.get("logger")
    for fn in DATASOURCE_JHU_GITHUB:
        if path.exists(DATASOURCE_JHU_GITHUB[fn]):
            os.remove(DATASOURCE_JHU_GITHUB[fn])
            logging.info(f'Removed:{DATASOURCE_JHU_GITHUB[fn]}')
    logging.info('Flow completed')

#run 4pm and 5pm daily
#daily_schedule = Schedule(clocks=[CronClock("30 1,2 * * *")])
#daily_schedule= (CronSchedule("58 17 * * *"))
#print(daily_schedule)

#Set up a prefect flow and run it on a schedule
#with Flow('COVID 19 flow', schedule=daily_schedule) as flow:
def main():
    #extract tasks
    #daily_covid_us_states_data = extract_us_covid19sheets_data()
    file_dfs = extract_gh_global_covid_ts_data()
    
    #transform
    daily_covid_data = transform_daily_covid_data(file_dfs)
    
    #load - add a load task and include it here
    cleanup_files()

main()
#flow.run()

