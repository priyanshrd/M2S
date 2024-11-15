import pymongo
import mysql.connector
from datetime import datetime
import json

# MongoDB setup
mongo_client = pymongo.MongoClient("mongodb://fanuc:1234@localhost:27017/MTLINKi")
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
    sample_document = mongo_collection.find_one()
    if not sample_document:
        print(f"No data found in the MongoDB collection: {collection}")
        continue

    # Create table query
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{collection}` ("
    field_definitions = []

    for field, value in sample_document.items():
        if field == "_id":
            field_definitions.append("`_id` VARCHAR(24) PRIMARY KEY")
        elif isinstance(value, str):
            field_definitions.append(f"`{field}` VARCHAR(255)")
        elif isinstance(value, int):
            field_definitions.append(f"`{field}` INT")
        elif isinstance(value, float):
            field_definitions.append(f"`{field}` FLOAT")
        elif isinstance(value, datetime):
            field_definitions.append(f"`{field}` DATETIME")
        elif isinstance(value, (list, dict)):
            field_definitions.append(f"`{field}` TEXT")
        else:
            field_definitions.append(f"`{field}` TEXT")

    create_table_query += ", ".join(field_definitions) + ")"
    sql_cursor.execute(create_table_query)

    # Insert data
    mongo_data = mongo_collection.find()

    for document in mongo_data:
        columns = []
        values = []
        for field, value in document.items():
            columns.append(f"`{field}`")

            try:
                if field == "_id":
                    values.append(f"'{str(value)}'")
                elif isinstance(value, datetime):
                    values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(value, str):
                    # Properly escape single quotes and handle potential binary data
                    escaped_value = value.replace("'", "\\'")
                    values.append(f"'{escaped_value}'")
                elif isinstance(value, (list, dict)):
                    # Convert to JSON string and escape properly
                    json_value = json.dumps(value, ensure_ascii=False).replace("'", "\\'")
                    values.append(f"'{json_value}'")
                else:
                    values.append(f"'{value}'" if value is not None else "NULL")
            except Exception as e:
                print(f"Error processing field '{field}': {e}")
                values.append("NULL")  # Fallback to NULL for problematic fields

    # Combine columns and values for the INSERT statement
        insert_query = f"INSERT IGNORE INTO `{collection}` ({', '.join(columns)}) VALUES ({', '.join(values)})"
        try:
            sql_cursor.execute(insert_query)
        except mysql.connector.errors.ProgrammingError as e:
            print(f"Error in collection '{collection}': {e}")
        except mysql.connector.errors.DataError as e:
            print(f"Data too large or malformed in collection '{collection}': {e}")


# Commit and close the connection
sql_connection.commit()
sql_cursor.close()
sql_connection.close()

print("Data transfer complete.")
