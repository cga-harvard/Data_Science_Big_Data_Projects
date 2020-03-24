import psycopg2
conn = psycopg2.connect(dbname=postgres, port=7435, user=postgres,password=postgres, host=localhost)
cur = conn.cursor()
print("Connected")
