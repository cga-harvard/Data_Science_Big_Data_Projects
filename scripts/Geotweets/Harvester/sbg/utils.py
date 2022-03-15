import configparser
import sys
import os
import psycopg2

def read_config():
    progname = sys.argv [0]
    if len (sys.argv) < 2:
        print ("Usage: " + progname + " <config_file>")
        raise SystemExit(1)

    config_file = sys.argv [1]

    if os.path.isfile (config_file) == False:
        print ("Error: Config file '" + config_file + "' not found.")
        raise SystemExit(1)

    config = configparser.RawConfigParser ()
    config.read_file (open (config_file))
    return config

def get_connection():
    config = read_config()

    dbhost = config.get ("DB credentials", "dbhost")
    dbport = config.get ("DB credentials", "dbport")
    dbname = config.get ("DB credentials", "dbname")
    dbuser = config.get ("DB credentials", "dbuser")
    dbpw = config.get ("DB credentials", "dbpw")
    db_conn = 'host=%s port=%s dbname=%s user=%s password=%s' % (dbhost, dbport,
            dbname, dbuser, dbpw)
    conn = psycopg2.connect(db_conn)
    return conn

def get_storage_table():
    config = read_config()
    table_name = config.get ("Storage params", "table")
    return table_name

def get_csv_location():
    config = read_config()
    csv_location = config.get ("Storage params", "csv_location")
    return csv_location

def get_output_location():
    config = read_config()
    output_loc = config.get("Storage params", "output_location")
    return output_loc

def get_db_params():
    config = read_config()
    dbhost = config.get("DB credentials", "dbhost")
    dbport = config.get("DB credentials", "dbport")
    dbname = config.get("DB credentials", "dbname")
    dbuser = config.get("DB credentials", "dbuser")
    dbpw = config.get("DB credentials", "dbpw")
    return [dbuser,dbpw,dbhost,dbport,dbname]
