import pymongo
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_ROOT_PASSWORD = os.getenv('MYSQL_ROOT_PASSWORD')

# MongoDB setup
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["Signal_values"]
mongo_collection = mongo_db["Machine_1_Signals"]

# MySQL setup
sql_connection = mysql.connector.connect(
    host="localhost",
    user="root",  
    password=MYSQL_ROOT_PASSWORD,  
    database="CNC_Signals"  
)
sql_cursor = sql_connection.cursor()

# Create table in MySQL if it doesn't exist
sql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS Machine_1_Signals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        machine_id VARCHAR(255),
        timestamp DATETIME,
        signal_type VARCHAR(255),
        signal_value FLOAT
    )
""")

# Retrieve data from MongoDB
mongo_data = mongo_collection.find()

# Insert data into MySQL
for document in mongo_data:
    machine_id = document.get("machine_id")
    timestamp = datetime.strptime(document.get("timestamp"), "%Y-%m-%dT%H:%M:%SZ")
    signal_type = document.get("signal_type")
    signal_value = document.get("signal_value")

    sql_cursor.execute("""
        INSERT INTO Machine_1_Signals (machine_id, timestamp, signal_type, signal_value)
        VALUES (%s, %s, %s, %s)
    """, (machine_id, timestamp, signal_type, signal_value))

# Commit and close the connection
sql_connection.commit()
sql_cursor.close()
sql_connection.close()

print("Data transfer complete.")
