'''
@file create_indices.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script will create coordinate and datetime
    indices for the target databases
'''
from diviner_tools import DatabaseTools
import glob
import os
import sys

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
    log = open(os.path.join(
        LOG_DIR, 'create_indices.log'),
        "w")
    sys.stdout = log

    # Create list of .db files in DB_DIR
    pattern = os.path.join(DB_DIR, '*.db')
    db_files = glob.glob(pattern)

    # For each db, create indices:
    #   - idx_clat_clon
    #   - idx_datetime
    dbt = DatabaseTools()

    for db in db_files:
        print("\nCreating idx_clat_clon for " + db)
        dbt.createCoordinateIndex(db)

        print("Creating idx_datetime for " + db)
        dbt.createDatetimeIndex(db)

    log.close()
    
if __name__ == "__main__":
    main()