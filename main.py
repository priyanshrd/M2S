import pymongo
import mysql.connector
from datetime import datetime
import json  # Import json to handle complex structures

# MongoDB setup
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["MTLINKi"]
mongo_collection_list = mongo_db.list_collection_names()

# MySQL setup
sql_connection = mysql.connector.connect(
    host="localhost",
    user="root",  # Update with your MySQL username
    password="root@123"  # Update with your MySQL password
)
sql_cursor = sql_connection.cursor()

# Create a new database in MySQL
sql_cursor.execute("CREATE DATABASE IF NOT EXISTS MTLINKi")
sql_cursor.execute("USE MTLINKi")

for collection in mongo_collection_list:
    mongo_collection = mongo_db[collection]
    # Get a sample document from MongoDB to infer fields
    sample_document = mongo_collection.find_one()
    if not sample_document:
        print(f"No data found in the MongoDB collection: {collection}")
        continue

    # Escape the collection name using backticks
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{collection}` ("
    field_definitions = []

    # Infer field names and types from the sample document, including `_id`
    for field, value in sample_document.items():
        # Escape field names using backticks
        if field == "_id":
            field_definitions.append("`_id` VARCHAR(24) PRIMARY KEY")  # Using VARCHAR(24) for ObjectId as primary key
        elif isinstance(value, str):
            field_definitions.append(f"`{field}` VARCHAR(255)")
        elif isinstance(value, int):
            field_definitions.append(f"`{field}` INT")
        elif isinstance(value, float):
            field_definitions.append(f"`{field}` FLOAT")
        elif isinstance(value, datetime):
            field_definitions.append(f"`{field}` DATETIME")
        elif isinstance(value, (list, dict)):  # Handle complex structures
            field_definitions.append(f"`{field}` TEXT")  # Store as JSON string
        else:
            field_definitions.append(f"`{field}` TEXT")  # Default to TEXT for unknown types

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
            columns.append(f"`{field}`")  # Escape field names
            # Handle value types
            if field == "_id":
                values.append(f"'{str(value)}'")  # Convert ObjectId to string and wrap in quotes
            elif isinstance(value, datetime):
                values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(value, str):
                # Escape single quotes in strings
                values.append(f"'{value.replace('\'', '\\\'')}'")
            elif isinstance(value, (list, dict)):
                # Convert lists or dictionaries to JSON strings
                values.append(f"'{json.dumps(value).replace('\'', '\\\'')}'")
            else:
                values.append(str(value))

        # Construct the INSERT query
        insert_query = f"INSERT IGNORE INTO `{collection}` ({', '.join(columns)}) VALUES ({', '.join(values)})"
        sql_cursor.execute(insert_query)

# Commit and close the connection
sql_connection.commit()
sql_cursor.close()
sql_connection.close()

print("Data transfer complete.")
