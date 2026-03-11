import json
import mysql.connector
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import List, Tuple
from config import DB_CONFIG, BATCH_SIZE


# ==========================================
# LOGGER SETUP
# ==========================================

os.makedirs("logs", exist_ok=True)

db_logger = logging.getLogger("db_logger")
db_logger.setLevel(logging.INFO)

log_file = "logs/db_queries.log"

handler = RotatingFileHandler(
    log_file,
    maxBytes=200 * 1024 * 1024,  # 200MB
    backupCount=5,
    encoding="utf-8"
)

formatter = logging.Formatter("%(message)s")
    

handler.setFormatter(formatter)

db_logger.addHandler(handler)
db_logger.propagate = False


# ==========================================
# DATABASE CONNECTION
# ==========================================

def get_connection():

    try:

        con = mysql.connector.connect(**DB_CONFIG)
        con.autocommit = False

        return con

    except Exception as e:

        db_logger.error(f"DB CONNECTION FAILED | {e}")
        return None


# ==========================================
# BATCH INSERT FUNCTION
# ==========================================

def batch_insert(
    cursor,
    con,
    insert_query: str,
    values: List[Tuple],
    batch_size: int = BATCH_SIZE,
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

            con.rollback()

            db_logger.warning(
                f"BATCH FAILED | rows {s}-{e} | error={exp}"
            )

            failed_batches.append(batch)

    return batch_count, failed_batches


# ==========================================
# INSERT FUNCTION
# ==========================================

def insert_into_db(cursor, con, parsed_batch: List[dict], table_name: str):

    if not parsed_batch:
        return

    try:

        columns = list(parsed_batch[0].keys())

        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_query = f"""
        INSERT INTO {table_name} ({cols})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {columns[0]}={columns[0]}
        """

        values = [tuple(row.values()) for row in parsed_batch]

        # ======================================
        # LOG SQL QUERIES (ESCAPED)
        # ======================================

        for row_values in values:

            formatted_vals = ", ".join(
                ("'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'")
                if isinstance(v, str)
                else "NULL"
                if v is None
                else str(v)
                for v in row_values
            )

            log_query = f"INSERT INTO {table_name} ({cols}) VALUES ({formatted_vals});"

            db_logger.info(log_query)

        # ======================================

        batch_insert(cursor, con, insert_query, values)

    except Exception as e:

        con.rollback()

        db_logger.error(f"INSERT FAILED | {e}")
