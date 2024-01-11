'''
@file find_urls.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)
 
@brief	We will extract the URLs for the zip files which contain tab files that contain the RDR LVL1 tables. 
		This can be a somewhat slow process, so we will do this once and then save the urls to a text file. 
		Skip this step if the ZIP_URLS_FILE already exists and has been populated. We expect 717,509 URLs.

		Each year takes approximately 30-45 seconds, so total time should be around 5-10 minutes.
'''
from diviner_tools import DivinerTools
import sys

def main():

	# Get CFG_FILEPATH and ZIP_FILEPATH from args
	if len(sys.argv) < 3:
		print("CFG_FILEPATH and ZIP_FILEPATH required")
		sys.exit(1)
	
	# Filepath to the yaml config file
	CFG_FILEPATH = sys.argv[1]

	# Filepath to list of .zip URLs
	ZIP_FILEPATH = sys.argv[2]

	# Init Diviner Tools
	dt = DivinerTools(CFG_FILEPATH)

	# Extract .zip URLs
	zip_urls = dt.findZipUrls()
	print("Found " + repr(len(zip_urls)) + " .zip file urls")

	# Save URLs to file
	dt.appendToFile(ZIP_FILEPATH, zip_urls)
	print("Saved to: " + ZIP_FILEPATH)


if __name__ == '__main__':
	main()
