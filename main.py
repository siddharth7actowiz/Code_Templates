import sys
import threading
from config import *
from utils import load_files
from parser import parse_json
from db import get_connection, insert_into_db


def process_chunk(start, end):

    con = get_connection()
    cursor = con.cursor()

    parsed_batch = []

    for raw_json in load_files(DATA_DIR, start, end):

        parsed = parse_json(raw_json)

        if not parsed:
            continue

        parsed_batch.append(parsed)

        if len(parsed_batch) >= BATCH_SIZE:

            insert_into_db(cursor, con, parsed_batch, TABLE_NAME)
            parsed_batch.clear()

    if parsed_batch:
        insert_into_db(cursor, con, parsed_batch, TABLE_NAME)

    cursor.close()
    con.close()


def main():

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    total_files = end - start
    chunk_size = total_files // TOTAL_THREADS

    threads = []

    for i in range(TOTAL_THREADS):

        chunk_start = start + i * chunk_size
        chunk_end = end if i == TOTAL_THREADS - 1 else chunk_start + chunk_size

        t = threading.Thread(
            target=process_chunk,
            args=(chunk_start, chunk_end)
        )

        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
