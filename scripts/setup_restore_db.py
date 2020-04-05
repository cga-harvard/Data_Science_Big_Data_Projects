import psycopg2

try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' port='7548'")
except:
    print "I am unable to connect to the database"
