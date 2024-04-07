'''
@file find_urls.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief Finds all .zip file URLs
'''
from diviner_tools import ZipCrawler, Utils
import sys


def main():

    # Get ZIP_FILEPATH from args
    if len(sys.argv) < 2:
        print("ZIP_FILEPATH required")
        sys.exit(1)

    # Filepath to list of .zip URLs
    ZIP_FILEPATH = sys.argv[1]

    # Init ZipCrawler and Utils
    zc = ZipCrawler()
    ut = Utils()

    '''
	We will extract the URLs for the zip files which contain tab files that contain the RDR LVL1 tables.
	This can be a somewhat slow process, so we will do this once and then save the urls to a text file.
	Skip this step if the ZIP_URLS_FILE already exists and has been populated. We expect 717,509 URLs.

	Each year takes approximately 30-45 seconds, so total time should be around 5-10 minutes.
	'''
    print("Starting web crawl...")
    zip_urls = zc.findZipUrls()
    print("Found " + repr(len(zip_urls)) + " .zip file urls")

    # Save URLs to file
    ut.appendToFile(ZIP_FILEPATH, zip_urls)
    print("Saved to: " + ZIP_FILEPATH)


if __name__ == '__main__':
    main()
