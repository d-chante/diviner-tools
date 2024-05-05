'''
@file collect_aoi_data.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script will collect AOI data from all databases
'''
from diviner_tools import ProfileGenerator
import os
import sys


def main():

    # Get DB_DIR from args
    if len(sys.argv) < 3:
        print("CFG_FILEPATH and AOI_CFG_FILEPATH required")
        sys.exit(1)

    # Path to config file
    CFG_FILEPATH = sys.argv[1]

    # Path to AOI config file
    AOI_CFG_FILEPATH = sys.argv[2]

    pg = ProfileGenerator(CFG_FILEPATH, AOI_CFG_FILEPATH)
    pg.collectData()

if __name__ == "__main__":
    main()
