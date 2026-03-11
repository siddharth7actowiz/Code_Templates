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



#--------------------Main with Parser json threads------------------------------------------------

import sys
import threading
import logging
import time

from config import *
from utils import load_files
from parser import parse_json
from db import get_connection, insert_into_db


# =========================
# LOGGER SETUP
# =========================

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)

handler.setFormatter(formatter)

logger.addHandler(handler)


# =========================
# BATCH PARSER
# =========================

def parse_batch(raw_batch, results):

    parsed_count = 0
    skipped_count = 0

    for raw_json in raw_batch:

        parsed = parse_json(raw_json)

        if parsed:
            results.append(parsed)
            parsed_count += 1
        else:
            skipped_count += 1

    logger.info(
        f"Batch parsed | parsed={parsed_count} skipped={skipped_count}"
    )


# =========================
# MULTITHREADED PARSER
# =========================

def threaded_parser(raw_files):

    threads = []
    results = []

    chunk_size = len(raw_files) // TOTAL_THREADS

    for i in range(TOTAL_THREADS):

        start = i * chunk_size
        end = len(raw_files) if i == TOTAL_THREADS - 1 else start + chunk_size

        chunk = raw_files[start:end]

        t = threading.Thread(
            target=parse_batch,
            args=(chunk, results),
            name=f"Parser-{i+1}"
        )

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return results


# =========================
# MAIN PIPELINE
# =========================

def main():

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    logger.info("Pipeline started")

    start_time = time.time()

    # ---------------------
    # LOAD FILES
    # ---------------------

    raw_files = list(load_files(DATA_DIR, start, end))

    logger.info(f"Loaded {len(raw_files)} files")

    # ---------------------
    # PARSE FILES
    # ---------------------

    parsed_results = threaded_parser(raw_files)

    logger.info(f"Total parsed records: {len(parsed_results)}")

    # ---------------------
    # DATABASE INSERT
    # ---------------------

    con = get_connection()
    cursor = con.cursor()

    inserted = 0

    for i in range(0, len(parsed_results), BATCH_SIZE):

        batch = parsed_results[i:i + BATCH_SIZE]

        insert_into_db(cursor, con, batch, TABLE_NAME)

        inserted += len(batch)

        logger.info(f"Inserted batch of {len(batch)} records")

    cursor.close()
    con.close()

    elapsed = time.time() - start_time

    logger.info(f"Pipeline finished | inserted={inserted} | time={elapsed:.2f}s")


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    main()
