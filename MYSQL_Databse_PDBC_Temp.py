import json
import mysql.connector
from typing import List, Tuple
from datetime import datetime


# ==========================================
# CONFIGURATION
# ==========================================

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "sid",
    "database": "grabfood",
    "port": 3306
}

BATCH_SIZE = 200


# ==========================================
# DATABASE CONNECTION
# ==========================================

def get_connection():
    con = mysql.connector.connect(**DB_CONFIG)
    con.autocommit = False
    return con


# ==========================================
# BATCH INSERT FUNCTION
# ==========================================

def batch_insert(cursor, insert_query: str, values: List[Tuple], batch_size: int = BATCH_SIZE):

    total_records = len(values)
    batch_count = 0
    failed_batches = []

    for start in range(0, total_records, batch_size):
        end = min(start + batch_size, total_records)
        batch = values[start:end]

        try:
            cursor.executemany(insert_query, batch)
            con.commit()
            batch_count += 1
            print(f"Inserted batch {batch_count} ({start} → {end})")

        except Exception as e:
            print(f"Batch failed ({start} → {end})")
            print("Error:", e)
            failed_batches.append(batch)

    return batch_count, failed_batches


# ==========================================
# DATA TRANSFORMATION FUNCTION
# ==========================================

def transform_json_to_db_values(json_data):

    restaurant_values = []
    menu_values = []

    for data in json_data:
        rest = data.get("Restaurant_Details", {})
        menu_items = data.get("Menu_Items", [])

        restaurant_values.append((
            rest.get("Restaurant_ID"),
            rest.get("Restaurant_Name"),
            rest.get("Branch_Name"),
            rest.get("Cuisine"),
            json.dumps(rest.get("Tip")),
            rest.get("Timezone"),
            rest.get("ETA"),
            json.dumps(rest.get("DeliveryOptions")),
            rest.get("Rating"),
            rest.get("Is_Open"),
            rest.get("Currency_Code"),
            rest.get("Currency_Symbol"),
            json.dumps(rest.get("Offers")),
            rest.get("Timing_Everyday")
        ))

        for item in menu_items:
            menu_values.append((
                item.get("Restaurant_ID"),
                item.get("Category_Name"),
                item.get("Item_ID"),
                item.get("Item_Name"),
                item.get("Item_Description"),
                item.get("Item_Price"),
                item.get("Item_Discounted_Price"),
                json.dumps(item.get("Item_Image_URL")),
                json.dumps(item.get("Item_Thumbnail_URL")),
                item.get("Item_Available"),
                item.get("Is_Top_Seller"),
            ))

    return restaurant_values, menu_values


# ==========================================
# MAIN EXECUTION FUNCTION
# ==========================================

def run_ingestion(json_file_path):

    start_time = datetime.now()

    # Load JSON
    with open(json_file_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    # Transform
    restaurant_values, menu_values = transform_json_to_db_values(json_data)

    # Queries
    insert_rest_query = """
    INSERT INTO RESTAURANT_TABLE (
        Restaurant_ID,
        Restaurant_Name,
        Branch_Name,
        Cuisine,
        Tip,
        Timezone,
        ETA,
        DeliveryOptions,
        Rating,
        Is_Open,
        Currency_Code,
        Currency_Symbol,
        Offers,
        Timing_Everyday
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert_menu_query = """
    INSERT INTO MENU_ITEMS_TABLE (
        Restaurant_ID,
        Category_Name,
        Item_Id,
        Item_Name,
        Item_Description,
        Item_Price,
        Item_Discounted_Price,
        Item_Image_URL,
        Item_Thumbnail_URL,
        Item_Available,
        Is_Top_Seller
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    con = get_connection()
    cursor = con.cursor()

    try:
        print("Starting Restaurant Batch Insert...")
        rest_batches, rest_failed = batch_insert(
            cursor, insert_rest_query, restaurant_values
        )

        print("Starting Menu Batch Insert...")
        menu_batches, menu_failed = batch_insert(
            cursor, insert_menu_query, menu_values
        )

        
        print("Transaction Successful")
        print(f"Restaurant batches: {rest_batches}")
        print(f"Menu batches: {menu_batches}")

        print("Failed Restaurant Rows:", sum(len(b) for b in rest_failed))
        print("Failed Menu Rows:", sum(len(b) for b in menu_failed))

    except Exception as e:
        con.rollback()
        print("Transaction Failed")
        print("Error:", e)

    finally:
        cursor.close()
        con.close()

    end_time = datetime.now()
    print("Execution Time:", end_time - start_time)


# ==========================================
# ENTRY POINT
# ==========================================

if __name__ == "__main__":
    run_ingestion("GRABFOOD_MENU_2026_02_26.json")
