import os

# ---------------- INPUT ----------------

project_name = input("Enter ETL  project name: ").strip()

print("\nSelect Project Type:")
print("1. Restaurant")
print("2. E-commerce")
print("3. Flights")
print("4. Properties(Hotels)")

ptype = input("Enter choice: ").strip()

multi = input("Enable DB multithreading? (y/n): ").strip().lower()
logging_needed = input("Enable logging? (y/n): ").strip().lower()

# ---------------- CREATE STRUCTURE ----------------

os.makedirs(project_name, exist_ok=True)
os.chdir(project_name)

if logging_needed == "y":
    os.makedirs("logs", exist_ok=True)

# ---------------- .env ----------------

open(".env", "w").write("""DATA_DIR
FILE_PATH
DB_USER
DB_PASSWORD
DB_HOST=localhost
DB_PORT=3306
DB_NAME
""")

# ---------------- CONFIG ----------------

open("config.py", "w").write("""from dotenv import load_dotenv
import os

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR")
FILE_PATH=os.getenv("FILE_PATH")                             

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME")
}

TABLE_NAME = "main_table"
""")

# ---------------- UTILS ----------------

open("utils.py", "w").write("""import json
import gzip
import os
from config import DATA_DIR   

#for multiple files from data dir                            
files = os.listdir(DATA_DIR)
files.sort()
start=0
files = os.listdir(DATA_DIR)
end=len(files)                                                                                 

def read_html(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print("Error read_html:", e)

def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print("Error read_json:", e)
        return {}

def read_gzip(path):
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)
    except Exception as e:
        print("Error read_gzip:", e)




def load_files(data_dir, start, end):
    
    for file in files[start:end]:
        path = os.path.join(data_dir, file)
        try:
            with gzip.open(path, "rt", encoding="utf-8",errors="replace") as f: #decompress
                yield json.load(f)

        except Exception as e:
            print("Error in func:", load_files.__name__, e)                                                         
""")

# ---------------- VALIDATION + SCHEMA ----------------

if ptype == "1":  # Restaurant
    validation = """from pydantic import BaseModel

class Store(BaseModel):
    Rest_Name: str
    Rest_Id: str
    Cuisines: str
    Address: str
    City: str
    State: str
    ETA: str
    Timing: str
    DeliveryOptions: str
    Region: str
    Pincode: str
    Url: str
    Desc: str
    MapUrl: str
    Image_Url: str = ""
    Ratings: float = 0.0
"""
    schema = """
        Rest_Name VARCHAR(200),
        Rest_Id VARCHAR(200) UNIQUE,
        Cuisines TEXT,
        Address TEXT,
        City VARCHAR(100),
        State VARCHAR(100),
        ETA VARCHAR(50),
        Timing VARCHAR(200),
        DeliveryOptions TEXT,
        Region VARCHAR(100),
        Pincode VARCHAR(20),
        Url TEXT,
        Desc TEXT,
        MapUrl TEXT,
        Image_Url TEXT,
        Ratings FLOAT
    """

elif ptype == "2":  # Ecommerce
    validation = """from pydantic import BaseModel

class Store(BaseModel):
    product_name: str
    product_id: str
    price: float
    discount: float
    discount_price: float
    ratings: float
    quantity: int
    currency: str
    image_url: str = ""
    product_url: str = ""
    brand: str = ""
    availability: str = ""
"""
    schema = """
        product_name VARCHAR(255),
        product_id VARCHAR(200) UNIQUE,
        price FLOAT,
        discount FLOAT,
        discount_price FLOAT,
        ratings FLOAT,
        quantity INT,
        currency VARCHAR(10),
        image_url TEXT,
        product_url TEXT,
        brand VARCHAR(100),
        availability VARCHAR(50)
    """

elif ptype == "3":  # Flights
    validation = """from pydantic import BaseModel

class Store(BaseModel):
    flight_name: str
    flight_id: str
    source: str
    destination: str
    departure_time: str
    arrival_time: str
    duration: str
    price: float
    currency: str
    airline: str = ""
    stops: str = ""
"""
    schema = """
        flight_name VARCHAR(200),
        flight_id VARCHAR(200) UNIQUE,
        source VARCHAR(100),
        destination VARCHAR(100),
        departure_time VARCHAR(50),
        arrival_time VARCHAR(50),
        duration VARCHAR(50),
        price FLOAT,
        currency VARCHAR(10),
        airline VARCHAR(100),
        stops VARCHAR(50)
    """

else:  # Airbnb
    validation = """from pydantic import BaseModel

class Store(BaseModel):
    property_name: str
    property_id: str
    location: str
    price_per_night: float
    rating: float
    amenities: str
    host_name: str
    property_type: str = ""
    availability: str = ""
    image_url: str = ""
"""
    schema = """
        property_name VARCHAR(200),
        property_id VARCHAR(200) UNIQUE,
        location VARCHAR(200),
        price_per_night FLOAT,
        rating FLOAT,
        amenities TEXT,
        host_name VARCHAR(100),
        property_type VARCHAR(100),
        availability VARCHAR(50),
        image_url TEXT
    """

open("validation.py", "w").write(validation)

# ---------------- PARSER ----------------

open("parser.py", "w").write("""from validation import Store

def parser(data):
       pass                      

    # try:
    #     obj = Store(**data)
    #     return obj.model_dump()
    # except Exception as e:
    #     print("Parse error:", e)
    #     return None
""")

# ---------------- DB ----------------

db_code = f"""import mysql.connector
from config import *
"""

if logging_needed == "y":
    db_code += """
import logging

db_logger = logging.getLogger("db")
db_logger.setLevel(logging.INFO)
db_logger.addHandler(logging.FileHandler("logs/db.log"))

query_logger = logging.getLogger("query")
query_logger.setLevel(logging.INFO)
query_logger.addHandler(logging.FileHandler("logs/query.log"))
"""

db_code += f"""
def make_connection():
    return mysql.connector.connect(**DB_CONFIG)

def create_table(cursor):
    cursor.execute(\"\"\"
    CREATE TABLE IF NOT EXISTS {{TABLE_NAME}}(
        id INT AUTO_INCREMENT PRIMARY KEY,
        {schema}
    )
    \"\"\")

def insert_into_db(cursor, con, data):

    if not data:
        return

    cols = list(data[0].keys())
    col_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))

    query = f"INSERT INTO {{TABLE_NAME}} ({{col_str}}) VALUES ({{placeholders}}) ON DUPLICATE KEY UPDATE id=id"

    values = [tuple(row.get(col) for col in cols) for row in data]

    try:
        cursor.executemany(query, values)
        con.commit()
"""

if logging_needed == "y":
    db_code += """
        db_logger.info(f"Inserted {len(data)} rows")
        query_logger.info(query)
"""

db_code += """
    except Exception as e:
        con.rollback()
"""

if logging_needed == "y":
    db_code += """
        db_logger.error(e)
"""

open("db.py", "w").write(db_code)

# ---------------- MAIN ----------------

if multi == "y":
    main_code = """import threading
from queue import Queue
from config import *
from utils import read_json
from parser import parser
from db import *

q = Queue()

def db_worker():
    con = make_connection()
    cursor = con.cursor()
    batch = []

    while True:
        data = q.get()
        if data is None:
            break

        batch.append(data)

        if len(batch) >= 100:
            insert_into_db(cursor, con, batch)
            batch.clear()

        q.task_done()

def main():
    con = make_connection()
    cursor = con.cursor()
    create_table(cursor)
    con.commit()
    cursor.close()
    con.close()

    t = threading.Thread(target=db_worker)
    t.start()

    for data in read_json(DATA_DIR):
        parsed = parser(data)
        if parsed:
            q.put(parsed)

    q.put(None)
    t.join()

if __name__ == "__main__":
    main()
"""
else:
    main_code = """from config import *
from utils import read_json
from parser import parser
from db import *

def main():
    st=time.time()
    data=read_html(FILE_PATH)
    extracted_data=parse_html(data)
    con = make_connection()
    cursor = con.cursor()

    create_table(cursor)

    data_list = []

    for data in read_json(DATA_DIR):
        parsed = parser(data)
        if parsed:
            data_list.append(parsed)

    insert_into_db(cursor, con, data_list)

    cursor.close()
    con.close()
    print(time.time()-st)
if __name__ == "__main__":
    main()
"""

open("main.py", "w").write(main_code)

# ---------------- DONE ----------------

print("✅ Project created successfully!")
os.system("code .")