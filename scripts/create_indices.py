'''
@file create_indices.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script will create coordinate and datetime
    indices for the target databases
'''
from diviner_tools import DatabaseTools
import concurrent.futures
import glob
import logging
import os
import sys


def createIndicesThread(db):

    dbt = DatabaseTools()

    logging.info("Creating idx_clat_clon for " + db)
    dbt.createIndex(db, "idx_clat_clon", ["CLAT", "CLON"])

    logging.info("Creating idx_datetime for " + db)
    dbt.createIndex(db, "idx_datetime", ["DATETIME"])


def main():

    # Get DB_DIR from args
    if len(sys.argv) < 3:
        print("DB_DIR and LOG_DIR required")
        sys.exit(1)

    # Directory to databases
    DB_DIR = sys.argv[1]

    # Directory to logs
    LOG_DIR = sys.argv[2]

    # Configure logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, 'create_indices.log')),
            logging.StreamHandler()])

    # Create list of .db files in DB_DIR
    pattern = os.path.join(DB_DIR, '*.db')
    db_files = glob.glob(pattern)

    # For each db, create indices:
    #   - idx_clat_clon
    #   - idx_datetime
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

        futures = [executor.submit(createIndicesThread, db) for db in db_files]

        results = [future.result()
                   for future in concurrent.futures.as_completed(futures)]


if __name__ == "__main__":
    main()
