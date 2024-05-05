'''
@file create_aoi_indices.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script will create coordinate 
    indices for all tables in the AOI database
'''
from diviner_tools import DatabaseTools
import sys


def main():

    # Get DB_DIR from args
    if len(sys.argv) < 2:
        print("DB_PATH required")
        sys.exit(1)

    # Directory to AOI database path
    DB_PATH = sys.argv[1]

    dbt = DatabaseTools()

    table_list = dbt.getTableList(DB_PATH)

    for table in table_list:
        idx_clat_clon_name = "idx_clat_clon_" + table

        print("Creating index " + idx_clat_clon_name)
        dbt.createIndex(
            database_path=DB_PATH, 
            index_name=idx_clat_clon_name, 
            columns=["CLAT", "CLON"], 
            table_name=table)

if __name__ == "__main__":
    main()
