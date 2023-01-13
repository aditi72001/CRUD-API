import psycopg2
import random

# Establishing the connection
try:
    connection = psycopg2.connect(
        user="postgres",
        password="adu72001",
        host="localhost",
        port=5432,
        database="testdb",
    )
    cursor = connection.cursor()
    print("POSTGRES  CONNECTED")
except:
    print("Not Connected")

all_batches_query = """ SELECT * from batches """
        cursor.execute(all_batches_query)
        records = cursor.fetchall()
        sql = '''CREATE TABLE IF NOT EXISTS public.BATCH(
                timestamp TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                arrivalTime TEXT ,
                carrierId TEXT,
                defect TEXT,
                vin TEXT,
                filePath TEXT
)'''
        cursor.execute(sql)
        print("Table created successfully........")
        connection.commit()
        
def insert(data_to_be_insert):
    insert_query = "INSERT INTO BATCH( timestamp,model, arrivalTime, carrierId,defect,vin,filePath) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    record_to_insert = data_to_be_insert
    try:
        for i in record_to_insert:
            cursor.execute(insert_query, i)
            connection.commit()
            count = cursor.rowcount
        print(count, "Record inserted successfully \
        into batch table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into batch table", error)

def truncate(tableName):
    s="DELETE from "
    command = s+tableName
    try:
        cursor.execute(command)
        connection.commit()
        cursor.close()
        print("table truncated")
    except (Exception, psycopg2.Error) as error:
        print("Failed to truncate record into batch table", error)



# insert([(1, 'Scorpion',random.randint(0,20) , random.randint(0,10),0,random.randint(0,10),0)])
# truncate("batch")



if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")