'''
@file data_preprocess.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief	This python script provides a procedure to pre-process channel 7 Diviner data
	collected between January 2010 - September 2023.
'''
from diviner_tools import DivinerPreprocessor, Utils
import sys


def main():

    # Get CFG_FILEPATH, ZIP_FILEPATH, and BATCH_ID from args
    if len(sys.argv) < 4:
        print("CFG_FILEPATH, ZIP_FILEPATH, and BATCH_ID required")
        sys.exit(1)

    # Filepath to the yaml config file
    CFG_FILEPATH = sys.argv[1]

    # Filepath to list of .zip URLs
    ZIP_FILEPATH = sys.argv[2]

    # Batch ID (0 to 72)
    BATCH_ID = int(sys.argv[3])

    # Master batch size
    M_BATCH_SIZE = 10000

    '''
    diviner_tools is a custom library developed specifically for this task. Upon initialization
    of the Diviner Preprocessor object, it will create the data directory and database if they don't
    already exist.
	'''
    ut = Utils()
    dp = DivinerPreprocessor(CFG_FILEPATH, "job_" + repr(BATCH_ID))

    '''
    Preprocessing will involve:
    * Splitting the zip file URLs into batches
    * For each url, download the .zip file to local directory
    * Unpack the .zip file
    * Read the lines from the unpacked .TAB file
    * Check each line against desired criteria (activity flag, geoemetry flag, etc)
    * If a line meets the desired criteria, write it to our database
    * If a .TAB file contains data that was written to the database, save the filename to a textfile
    * Delete the .TAB file
    
    Since there is a lot of data to process which may take a long period of time, we will split
    the 717,509 URLs into parent batches and will manually start each master batch.
    '''
    all_urls = ut.txtToList(ZIP_FILEPATH)

    # Master batches
    master_batches = ut.batch(all_urls, M_BATCH_SIZE)

    # Pre-process loop
    dp.preprocess(master_batches[BATCH_ID])


if __name__ == "__main__":
    main()
