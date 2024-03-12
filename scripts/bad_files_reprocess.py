'''
@file bad_files_reprocess.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)
 
@brief	This python script provides a procedure to pre-process channel 7 Diviner data 
	collected between January 2010 - September 2023.
'''
from diviner_tools import DivinerPreprocessor, Utils
import sys

def main():

	# Get CFG_FILEPATH
	if len(sys.argv) < 3:
		print("CFG_FILEPATH and BAD_FILES_DIR required")
		sys.exit(1)
	
	# Filepath to the yaml config file
	CFG_FILEPATH = sys.argv[1]
	
    # Directory to bad files 
	BAD_FILES_DIR = sys.argv[2]

	ut = Utils()
	dp = DivinerPreprocessor(CFG_FILEPATH, "bad_files")
	
	bad_files_txts = ut.getAllFilenamesFromDir(BAD_FILES_DIR)

	bad_urls = []
	
	for file in bad_files_txts:
		bad_urls += ut.txtToList(file)

	print(repr(bad_urls))
        
	#dp.preprocess(master_batches[BATCH_ID])

if __name__ == "__main__":
	main()
