'''
@file bad_files_reprocess.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script will attempt to re-process files that
	had failed to process in the original pre-processing pass
'''
from diviner_tools import DivinerPreprocessor, Utils
import sys


def main():

    # Get CFG_FILEPATH, ZIP_FILEPATH, and BATCH_ID from args
    if len(sys.argv) < 4:
        print("CFG_FILEPATH, BAD_FILES_DIR, and ZIP_FILEPATH required")
        sys.exit(1)

    # Filepath to the yaml config file
    CFG_FILEPATH = sys.argv[1]

    # Directory to bad files
    BAD_FILES_DIR = sys.argv[2]

    # Filepath to list of .zip URLs
    ZIP_FILEPATH = sys.argv[3]

    ut = Utils()
    dp = DivinerPreprocessor(CFG_FILEPATH, "bad_files")

    # Get a list of bad_files text flies
    bad_file_urls = ut.getAllFilenamesFromDir(BAD_FILES_DIR)

    # Pre-process loop
    dp.preprocess(bad_file_urls)

if __name__ == "__main__":
    main()
