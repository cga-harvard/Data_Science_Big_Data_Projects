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

conn.execute("""
                CREATE TABLE IF NOT EXISTS Indicator_District_Data
                (
                    District_Name TEXT ENCODING DICT(32),
                    Indicator_Name TEXT ENCODING DICT(32),
                    District_ID FLOAT,
                    Indicator_ID FLOAT,
                    Prevalence_2021 FLOAT,
                    Prevalence_Rank_2021 FLOAT,
                    Prevalence_Decile_2021 FLOAT,
                    Headcount_2021 FLOAT,
                    Headcount_Rank_2021 FLOAT,
                    Headcount_Decile_2021 FLOAT,
                    Prevalence_2016 FLOAT,
                    Prevalence_Rank_2016 FLOAT,
                    Prevalence_Decile_2016 FLOAT,
                    Prevalence_Change FLOAT,
                    Prevalence_Change_Rank FLOAT,
                    Prevalence_Change_ID FLOAT,
                    Prevalence_Change_Name TEXT ENCODING DICT(32),
                    State_ID FLOAT,
                    State_Name TEXT ENCODING DICT(32)
                );
            """)

for filename in all_files:
    print(f"Processing file: {filename}")
    try:
        df = pd.read_csv(filename, encoding='latin1')
        #print(df.columns.to_list())
    except Exception as e:
        l_ni.append(filename)
        print(f"Corrupt file {filename}: {e}")
        continue

    string_columns = ['ï»¿District Name', 'Indicator Name', 'Prevalence Change Name', 'State Name']
    df[string_columns] = df[string_columns].astype(str)

    # Remove 'th' suffix and convert to float for columns containing 'Decile'
    decile_columns = [col for col in df.columns if 'Decile' in col]
    for col in decile_columns:
        df[col] = df[col].str.replace('th', '').astype(float)

        # Fill NaN values in decile columns with 0
        df[col].fillna(0, inplace=True)

    # Convert the rest of the columns to float, excluding the string and decile columns
    float_columns = [col for col in df.columns if col not in string_columns]
    df[float_columns] = df[float_columns].astype(float)

    # Fill NaN values in all float columns with 0
    df[float_columns] = df[float_columns].fillna(0)

    # Fill NaN values in string columns with empty string
    df[string_columns] = df[string_columns].fillna('')

    # Rename columns
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
            conn.load_table_columnar("Indicator_District_Data", df, preserve_index=False)
            print(f"Inserted columnar {filename}")
        except Exception as inner_e:
            l_ni.append(filename)
            print(f"Not inserted {filename}: {inner_e}")

print("Files not inserted:", l_ni)