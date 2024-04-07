'''
@file bad_files_reprocess.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script will attempt to re-process files that
	had failed to process in the original pre-processing pass
'''
from diviner_tools import DivinerPreprocessor, Utils
import os
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
    bad_files_txts = ut.getAllFilenamesFromDir(BAD_FILES_DIR)

    # Read each text file to get the bad file paths
    bad_filepaths = []

    for file in bad_files_txts:
        bad_filepaths += ut.txtToList(os.path.join(BAD_FILES_DIR, file))

    # Extract the filenames
    bad_filenames = []

    for file in bad_filepaths:
        bad_filenames.append(file.split("/")[-1].split(".")[-2].lower())

    # Find the corresponding URL
    # Note: There is an issue opened to update DivinerPreprocessor so
    # that in the future, the bad file's URL is saved and not the
    # local filepath, whicih would allow us to skip this step
    zip_urls = ut.txtToList(ZIP_FILEPATH)

    bad_file_urls = []

    for filename in bad_filenames:
        bad_file_urls += [url for url in zip_urls if filename in url]

    # Pre-process loop
    dp.preprocess(bad_file_urls)


if __name__ == "__main__":
    main()
