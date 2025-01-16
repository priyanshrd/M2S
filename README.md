# MongoDB to MySQL Data Transfer Script

This repository contains a Python script to automate the transfer of data from a MongoDB database to a MySQL database. The script dynamically creates tables in MySQL, handling various data types from MongoDB documents.

---

[Integration of Mtlinki-MongoDB-SQL-PowerBI.pdf](https://github.com/user-attachments/files/18444847/Integration.of.Mtlinki-MongoDB-SQL-PowerBI.pdf)


## Setup Instructions

### Step 1: Setting Up MongoDB and MongoDB Compass

1. **Download and Install MongoDB:**
   - Download MongoDB from [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community).
   - Follow the installation guide for your operating system.

2. **Download and Install MongoDB Compass:**
   - Download MongoDB Compass from [https://www.mongodb.com/products/compass](https://www.mongodb.com/products/compass).
   - Install it following the provided instructions for your platform.

3. **Import Dummy JSON Data into MongoDB Using Compass:**
   1. Open MongoDB Compass and connect to `mongodb://localhost:27017/`.
   2. Click on **"Create Database"** and name it `MTLINKi`.
   3. Within the `MTLINKi` database, click **"Create Collection"** and give it a name (e.g., `MachineData`).
   4. Click on the newly created collection.
   5. Click **"Add Data"** > **"Import JSON"**, then select and import your dummy JSON file.

---

### Step 2: Setting Up the Python Script

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/priyanshrd/M2S.git
   cd M2S
    ```

2. **Install Dependencies:**
   Make sure you have Python installed on your system. Then, install the required packages:
    ```bash
    pip install -r requirements.txt
    ```
3. **Update Database Credentials:**
   Open main.py and modify the MongoDB and MySQL credentials:

    ```python
    # MongoDB setup
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    
    # MySQL setup
    sql_connection = mysql.connector.connect(
        host="localhost",
        user="root",  # Your MySQL username
        password= "Your MySQL password"
    )
    ```
### Step 3: Running the Script
Run the script to transfer data from MongoDB to MySQL:
```bash
python main.py
```
The script will create tables in the MySQL database MTLINKi and populate them with data from the MongoDB collections.
### Step 4: Verify Data in MySQL
Open your MySQL management tool (e.g., MySQL Workbench or phpMyAdmin).
Check the MTLINKi database to confirm that tables and data have been transferred correctly.
