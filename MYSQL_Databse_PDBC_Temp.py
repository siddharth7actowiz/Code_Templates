import json
import mysql.connector
from typing import List, Tuple
from datetime import datetime
from config import DB_CONFIG
# ==========================================
# DATABASE CONNECTION
# ==========================================

def get_connection():
    try:
        con = mysql.connector.connect(**DB_CONFIG)
        con.autocommit = False
        return con
    except Execption as e:
        print("Error in Db Connection",get_connection.__name__,e)

def create_table(Table_N,cursor):
    ddl=f'''
            CREATE TABLE IF NOT EXISTS {table_name}();
            
        '''
    cursor.execute(create_query)

            
    

# ==========================================
# BATCH INSERT FUNCTION
# ==========================================
def batch_insert(
    cursor,
    con,
    insert_query: str,
    values: List[Tuple],
    batch_size: int = db_batches,
) -> Tuple[int, list]:

    total_records = len(values)
    batch_count = 0
    failed_batches = []

    for s in range(0, total_records, batch_size):
        e = min(s + batch_size, total_records)
        batch = values[s:e]

        try:
            cursor.executemany(insert_query, batch)
            con.commit()
            batch_count += 1

        except Exception as exp:
            print(f"[BATCH FAILED] rows {s} to {e}  {exp}")
            failed_batches.append(batch)

    return batch_count, failed_batches



def insert_into_db(cursor, con, parsed_batch: List[dict], TABLE_NAME: str):
    try:
        if not parsed_batch:
            return

        cols = ", ".join(parsed_batch[0].keys())
        placeholders = ", ".join(["%s"] * len(parsed_batch[0]))

        insert_query = f"""
            INSERT INTO {TABLE_NAME} ({cols})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE col = col;
        """

        values = [tuple(row.values()) for row in parsed_batch]

       
        for row_values in values:
            formatted_vals = ", ".join(
                ("'" + str(v).replace("'", "''") + "'") if isinstance(v, str) else "NULL" if v is None else str(v)
                for v in row_values
            )
            print(f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({formatted_vals}) ON DUPLICATE KEY UPDATE Res_Id = Res_Id;")

        batch_insert(cursor, con, insert_query, values)

    except Exception as e:
        print(f"Error in insert_into_db: {e}")
