from glob import glob
from os import chdir,getcwd
import psycopg2
import datetime
import pandas as pd
from sqlalchemy import create_engine

from utils import get_csv_location
from utils import get_storage_table
from utils import get_connection
from utils import get_output_location
from utils import get_db_params

def create_table(conn, table_name,pkey):
    sql = """
        CREATE TABLE IF NOT EXISTS {0}
        (
        message_id bigint NOT NULL,
        tweet_date bigint,
        tweet_text character varying COLLATE pg_catalog."default",
        tags character varying COLLATE pg_catalog."default",
        tweet_lang character varying COLLATE pg_catalog."default",
        source character varying COLLATE pg_catalog."default",
        place character varying COLLATE pg_catalog."default",
        retweets bigint,
        tweet_favorites bigint,
        photo_url character varying COLLATE pg_catalog."default",
        quoted_status_id bigint,
        user_id character varying COLLATE pg_catalog."default",
        user_name character varying COLLATE pg_catalog."default",
        user_location character varying COLLATE pg_catalog."default",
        followers bigint,
        friends bigint,
        user_favorites bigint,
        status bigint,
        user_lang character varying COLLATE pg_catalog."default",
        latitude double precision,
        longitude double precision,
        data_source smallint[],
        gps boolean,
        spatialerror double precision,
        reply_to_user_id character varying COLLATE pg_catalog."default",
        reply_to_tweet_id character varying COLLATE pg_catalog."default",
        place_id character varying COLLATE pg_catalog."default",
        CONSTRAINT {1} PRIMARY KEY (message_id)
        )
        WITH (
            OIDS = FALSE
        )
        TABLESPACE pg_default;

        ALTER TABLE {0}
            OWNER to postgres;

        GRANT ALL ON TABLE {0} TO postgres WITH GRANT OPTION;
        """.format(table_name,pkey)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def create_harvard_table(conn, table_name):
    sql = """
        CREATE TABLE IF NOT EXISTS {0}
        (
        message_id bigint NOT NULL,
        tweet_date TIMESTAMP,
        tweet_text character varying COLLATE pg_catalog."default",
        tweet_lang character varying COLLATE pg_catalog."default",
        source character varying COLLATE pg_catalog."default",
        user_id character varying COLLATE pg_catalog."default",
        user_name character varying COLLATE pg_catalog."default",
        latitude double precision,
        longitude double precision,
        reply_to_user_id character varying COLLATE pg_catalog."default",
        reply_to_tweet_id character varying COLLATE pg_catalog."default",
        place_id character varying COLLATE pg_catalog."default",
        goog_x double precision,
        goog_y double precision,
        CONSTRAINT cga_pkey PRIMARY KEY (message_id)
        )
        WITH (
            OIDS = FALSE
        )
        TABLESPACE pg_default;

        ALTER TABLE {0}
            OWNER to postgres;

        GRANT ALL ON TABLE {0} TO postgres WITH GRANT OPTION;
        """.format(table_name)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def create_salzburg_table(conn,table_name):
    sql = """
    CREATE TABLE IF NOT EXISTS {0}
        (
        message_id bigint NOT NULL,
        date TIMESTAMP,
        text character varying COLLATE pg_catalog."default",
        tags character varying COLLATE pg_catalog."default",
        tweet_lang character varying COLLATE pg_catalog."default",
        source character varying COLLATE pg_catalog."default",
        place character varying COLLATE pg_catalog."default",
        geom character varying COLLATE pg_catalog."default",
        retweets bigint,
        tweet_favorites bigint,
        photo_url character varying COLLATE pg_catalog."default",
        quoted_status_id bigint,
        user_id character varying COLLATE pg_catalog."default",
        user_name character varying COLLATE pg_catalog."default",
        user_location character varying COLLATE pg_catalog."default",
        followers bigint,
        friends bigint,
        user_favorites bigint,
        status bigint,
        user_lang character varying COLLATE pg_catalog."default",
        latitude double precision,
        longitude double precision,
        data_source smallint[],
        gps boolean,
        spatialerror double precision,
        CONSTRAINT salzburg_pkey PRIMARY KEY (message_id)
        )
        WITH (
            OIDS = FALSE
        )
        TABLESPACE pg_default;

        ALTER TABLE {0}
            OWNER to postgres;

        GRANT ALL ON TABLE {0} TO postgres WITH GRANT OPTION;
        """.format(table_name)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def add_to_cga_table(csv_file, table,conn):
    cur = conn.cursor()

    sql = ("""
    CREATE TEMP TABLE tmp_table
    ON COMMIT DROP
    AS
    SELECT *
    FROM {0}
    WITH NO DATA;
    
    COPY tmp_table (
    message_id, tweet_date, latitude, longitude, goog_x, goog_y, user_id, user_name, source,
    reply_to_user_id, reply_to_tweet_id, place_id, tweet_text,tweet_lang)
    FROM
    '{1}'
    DELIMITER ',' CSV HEADER;
    
    INSERT INTO {0}
    SELECT *
    FROM tmp_table
    ON CONFLICT DO NOTHING;
    """.format(table, getcwd()+"/"+csv_file))
    try:
        cur.execute(sql)
    except psycopg2.DataError as e:
        print('csv_file: ' + csv_file)
        print(e)
    conn.commit()

def add_to_sbg_table(file, table,db_params):
    user,pwd,host,port,db = db_params
    alchemy_engine = create_engine('postgresql+psycopg2://'+user+":"+pwd+"@"+host+":"+port+"/"+db)
    data_frame = pd.read_csv(file, sep='\t', low_memory=False, error_bad_lines=False,
                                 lineterminator='\n', compression='gzip')
    data_frame.columns = map(str.lower, data_frame.columns)
    data_frame.to_sql('salzburg_table', alchemy_engine, if_exists='append', chunksize=10000, index=False)

def merge(conn):
    create_func = ("""
    CREATE OR REPLACE FUNCTION as_epoch(ts TIMESTAMP) RETURNS BIGINT AS $$
    BEGIN
        RETURN (EXTRACT(EPOCH FROM ts) * 1000)::BIGINT;
    END;
    $$ LANGUAGE plpgsql;
    """
    )
    curr = conn.cursor()
    curr.execute(create_func)
    conn.commit()

    sql = ("""
    INSERT INTO merged_table (message_id, tweet_date, tweet_text, tags, tweet_lang, source, place, 
        retweets, tweet_favorites, photo_url, quoted_status_id, user_id, user_name, user_location, followers, 
        friends, user_favorites, status, user_lang, latitude, longitude, data_source, gps, spatialerror)
    SELECT message_id, as_epoch(date), text, tags, tweet_lang, source, place, 
        retweets, tweet_favorites, photo_url, quoted_status_id, user_id, user_name, user_location, followers, 
        friends, user_favorites, status, user_lang, latitude, longitude, data_source, gps, spatialerror
    FROM salzburg_table;
    
    
    INSERT INTO merged_table (message_id, tweet_date, latitude, longitude, user_id, user_name, source,
        reply_to_user_id, reply_to_tweet_id, place_id, tweet_text,tweet_lang)
    SELECT message_id, as_epoch(tweet_date), latitude, longitude, user_id, user_name, source,
        reply_to_user_id, reply_to_tweet_id, place_id, tweet_text,tweet_lang
    FROM cga_table
    ON CONFLICT DO NOTHING;
    """
    )
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def add_to_final_db(conn):
    sql = ("""
    INSERT INTO final_db (SELECT * FROM merged_table) ON CONFLICT DO NOTHING;;
    """
    )
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def export(conn,file_name,output_loc):
    sql = ("""
    COPY merged_table TO '{1}/{0}.csv' WITH (FORMAT CSV, HEADER TRUE);
    """
    ).format(file_name,output_loc)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def empty_table(conn, table_name):
    sql = """
            TRUNCATE TABLE {0};
            """.format(table_name)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

if __name__ == '__main__':
    previous_day = list(str(datetime.date.today()-datetime.timedelta(1)))
    for index in range(0,len(previous_day)):
        if previous_day[index] == "-":
            previous_day[index] = "_"
    previous_day = "".join(previous_day)

    salzburg_location = get_csv_location() + '/sbg/'
    cga_location = get_csv_location() + '/cga/'
    final_table = get_storage_table()
    output_loc = get_output_location()
    db_params = get_db_params()

    salzburg_table_name = "Salzburg_table"
    cga_table_name = "cga_table"
    conn = get_connection()
    create_table(conn, final_table,"merged_pkey")
    create_table(conn, "final_db", "pkey")

    salzburg_files, cga_files, temp_files = [],[], []

    chdir(salzburg_location)
    temp_files = list(glob("*.gz"))
    temp_files.sort()
    for file in temp_files:
        if file[6] == '_':
            if file[0:5] + "0" + file[5:9] == previous_day:
                salzburg_files += [file]
        else:
            if file[0:10] == previous_day:
                salzburg_files += [file]
    create_salzburg_table(conn, salzburg_table_name)
    empty_table(conn, salzburg_table_name)

    chdir(cga_location)
    temp_files = list(glob("*.csv"))
    temp_files.sort()
    for file in temp_files:
        if file[16:26] == previous_day:
            cga_files += [file]
    create_harvard_table(conn, cga_table_name)

    index_salzburg,index_cga = 0,0
    cga_added, salzberg_added = False, False
    file_name = ""
    for time in range(0, 24):
        if index_salzburg == len(salzburg_files) and index_cga == len(cga_files):
            break

        if index_cga == len(cga_files) or int(cga_files[index_cga][-6:-4]) != time:
            cga_added = False
        else:
            chdir(cga_location)
            add_to_cga_table(cga_files[index_cga], cga_table_name, conn)
            cga_added = True
            index_cga += 1

        if index_salzburg == len(salzburg_files) or int(salzburg_files[index_salzburg][10:12]) != time:
            salzberg_added = False
        else:
            chdir(salzburg_location)
            add_to_sbg_table(salzburg_files[index_salzburg], salzburg_table_name,db_params)
            salzberg_added = True
            index_salzburg += 1

        if not cga_added and not salzberg_added:
            continue
        elif cga_added:
            file_name = cga_files[index_cga-1][16:-4]
        else:
            file_name = salzburg_files[index_salzburg-1][:-7]

        merge(conn)
        export(conn, file_name, output_loc)
        add_to_final_db(conn)

        empty_table(conn, "merged_table")
        empty_table(conn, cga_table_name)
        empty_table(conn, salzburg_table_name)