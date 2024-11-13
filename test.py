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

# Create a new database in MySQL
sql_cursor.execute("CREATE DATABASE IF NOT EXISTS CNC_Signals")
sql_cursor.execute("USE CNC_Signals")

# Get a sample document from MongoDB to infer fields
sample_document = mongo_collection.find_one()
if not sample_document:
    print("No data found in the MongoDB collection.")
    exit()

# Create the SQL table dynamically, including the `_id` field
create_table_query = "CREATE TABLE IF NOT EXISTS Machine_1_Signals (id INT AUTO_INCREMENT PRIMARY KEY, "
field_definitions = []

# Infer field names and types from the sample document, including `_id`
for field, value in sample_document.items():
    if field == "_id":
        field_definitions.append("_id VARCHAR(24)")  # Using VARCHAR(24) for ObjectId

    elif isinstance(value, str):
        field_definitions.append(f"{field} VARCHAR(255)")
    elif isinstance(value, int):
        field_definitions.append(f"{field} INT")
    elif isinstance(value, float):
        field_definitions.append(f"{field} FLOAT")
    elif isinstance(value, datetime):
        field_definitions.append(f"{field} DATETIME")
    else:
        field_definitions.append(f"{field} TEXT")  # Default to TEXT for unknown types

# Combine field definitions into the final query
create_table_query += ", ".join(field_definitions) + ")"
sql_cursor.execute(create_table_query)

# Retrieve data from MongoDB and insert into MySQL
mongo_data = mongo_collection.find()

# Insert data into MySQL
for document in mongo_data:
    columns = []
    values = []

    for field, value in document.items():
        columns.append(field)
        # Handle value types
        if field == "_id":
            values.append(f"'{str(value)}'")  # Convert ObjectId to string and wrap in quotes
        elif isinstance(value, datetime):
            values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
        elif isinstance(value, str):
            values.append(f"'{value}'")
        else:
            values.append(str(value))

    # Construct the INSERT query
    insert_query = f"INSERT INTO Machine_1_Signals ({', '.join(columns)}) VALUES ({', '.join(values)})"
    sql_cursor.execute(insert_query)

# Commit and close the connection
sql_connection.commit()
sql_cursor.close()
sql_connection.close()

print("Data transfer complete.")
