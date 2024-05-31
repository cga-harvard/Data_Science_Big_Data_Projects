import pandas as pd
from heavyai import connect
import glob

path = '/home/ubuntu/indicator-data/v2'  # Use your path
all_files = glob.glob(path + "/*.csv")

print("Connecting to Heavy")
conn = connect(user="admin", password="HarvardHe4vy1Q!", host="199.94.60.209", port=6274, dbname="heavyai")  # Use your port number
print("Connected", conn)
l_ni = []

# Use double quotes to quote the table name
conn.execute('DROP TABLE IF EXISTS "Indicator_District_Data";')

for filename in all_files:
    print(f"Processing file: {filename}")
    try:
        df = pd.read_csv(filename, encoding='latin1')
    except Exception as e:
        l_ni.append(filename)
        print(f"Corrupt file {filename}: {e}")
        continue

    # Fill NA values in integer columns with a placeholder value
    #int_columns = ['District ID', 'Indicator ID', 'Prevalence Rank 2021', 'Headcount Rank 2021', 'Prevalence Rank 2016', 'Prevalence Change Rank ', 'Prevalence Change ID', 'State ID']
    #df[int_columns] = df[int_columns].fillna(0).astype(int)

    values = {
        'ï»¿District Name': '', 'Indicator Name': '', 'District ID': 0, 'Indicator ID': 0, 'Prevalence 2021': 0.0, 'Prevalence Rank 2021': 0, 
        'Prevalence Decile 2021': '', 'Headcount 2021': 0.0, 'Headcount Rank 2021': 0, 'Headcount Decile 2021': '', 'Prevalence 2016': 0.0, 
        'Prevalence Rank 2016': 0, 'Prevalence Decile 2016': '', 'Prevalence Change': 0.0, 'Prevalence Change Rank ': 0, 
        'Prevalence Change Category ': '', 'Prevalence Change ID': 0, 'Prevalence Change Name': '', 'State ID': 0, 'State Name': ''
    }
    df.fillna(value=values, inplace=True)
    df = df.rename(columns={
        'ï»¿District Name': 'District_Name',
        'Indicator Name': 'Indicator_Name',
        'District ID': 'District_ID',
        'Indicator ID': 'Indicator_ID',
        'Prevalence 2021': 'Prevalence_2021',
        'Prevalence Rank 2021': 'Prevalence_Rank_2021',
        'Prevalence Decile 2021': 'Prevalence_Decile_2021',
        'Headcount 2021': 'Headcount_2021',
        'Headcount Rank 2021': 'Headcount_Rank_2021',
        'Headcount Decile 2021': 'Headcount_Decile_2021',
        'Prevalence 2016': 'Prevalence_2016',
        'Prevalence Rank 2016': 'Prevalence_Rank_2016',
        'Prevalence Decile 2016': 'Prevalence_Decile_2016',
        'Prevalence Change': 'Prevalence_Change',
        'Prevalence Change Rank ': 'Prevalence_Change_Rank',
        'Prevalence Change Category ': 'Prevalence_Change_Category',
        'Prevalence Change ID': 'Prevalence_Change_ID',
        'Prevalence Change Name': 'Prevalence_Change_Name',
        'State ID': 'State_ID', 
        'State Name': 'State_Name'      
    })
    
    try:
        conn.load_table("Indicator_District_Data", df, create='infer', method='arrow')
        print(f"Inserted Arrow {filename}")
    except Exception as e:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS Indicator_District_Data
                (
                    District_Name TEXT ENCODING DICT(32),
                    Indicator_Name TEXT ENCODING DICT(32),
                    District_ID INTEGER,
                    Indicator_ID INTEGER,
                    Prevalence_2021 FLOAT,
                    Prevalence_Rank_2021 INTEGER,
                    Prevalence_Decile_2021 TEXT ENCODING DICT(32),
                    Headcount_2021 BIGINT,
                    Headcount_Rank_2021 INTEGER,
                    Headcount_Decile_2021 TEXT ENCODING DICT(32),
                    Prevalence_2016 FLOAT,
                    Prevalence_Rank_2016 INTEGER,
                    Prevalence_Decile_2016 TEXT ENCODING DICT(32),
                    Prevalence_Change FLOAT,
                    Prevalence_Change_Rank INTEGER,
                    Prevalence_Change_Category TEXT ENCODING DICT(32),
                    Prevalence_Change_ID INTEGER,
                    Prevalence_Change_Name TEXT ENCODING DICT(32),
                    State_ID INTEGER,
                    State_Name TEXT ENCODING DICT(32)
                );
            """)
            conn.load_table_columnar("Indicator_District_Data", df, preserve_index=False)
            print(f"Inserted columnar {filename}")
        except Exception as inner_e:
            l_ni.append(filename)
            print(f"Not inserted {filename}: {inner_e}")

print("Files not inserted:", l_ni)