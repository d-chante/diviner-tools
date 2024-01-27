# Diviner Tools
Diviner Tools is a custom library for data pre-processing Diviner RDR LVL1 Channel 7 data that filters for data with the following parameters:
* Activity Flag (af) == 110
* Channel (c) == 7
* Emission Angle (cemis) < 10
* Calibration Quality Flag (qca) == 000
* Geometry Flag (qge) == 012
* Miscellaneous Quality Flag (qmi) == 000

The resulting database(s) contains a table with the following columns:
* year
* month
* day
* hour
* minute
* second
* sundist
* sunlat
* sunlon
* radiance
* tb
* clat
* clon
* cemis
* csunzen
* csunazi
* cloctime

Refer to the DLRE RDR Software Interface Specification for more details: https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1001/document/diviner_rdr_sis.pdf

### Requirements
The following packages are required:
* Python 3.10+
* beautifulsoup4
* public
* requests
* PyYAML

Install from the requirements file using pip:

	pip install -r /support/other/requirements.txt

### Finding all .Zip file URLs
The python script 'find_urls.py' will crawl the Diviner data host pages to find all available .zip urls. It starts at either:
* https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1001/data/ which contains data from 2009 to 2016, or
* https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1002/data/ which contains data from 2017 to the most recent release

To generate a text file that contains all discovered .zip file URLs, run the following:

    python3 find_urls.py </path/to/output/txt/file.txt>

This process will take approximately 10 minutes.

### Finding .Zip URLs from a specific year

You can also search for .zip urls from a specific year. First, import ZipCrawler and Utils from diviner_tools:

	from diviner_tools import ZipCrawler, Utils

Initialize both objects:

	zc = ZipCrawler()
	ut = Utils()

Then search for zip files for the specific year, for example, 2010:

	zip_urls = zc.findZipUrls("2010")

The object zip_urls will be a list of strings that are all of the relevant URLs. You can save this list to a file using:

	ut.appendToFile(</file/path.txt>, zip_urls)

### Preprocessing Data

There are 717,509 .zip URLs for the time period between 1 January 2010 to 15 September 2023. Each zip file is approximately 42 MB, and the uncompressed TAB files containing data points are 303 MB uncompressed. The pre-processing is done in a cyclical manner involving: downloading a zip file, unpacking it, deleting the original zip file, processing the contents of the tab file, writing target datapoints to a database, then deleting the tab file. This is to minimize storage requirements.

The pre-processing is split into master batches of 10,000 zip urls which results in 72 jobs being required to run. Each job will use about 15 GB of RAM, 12 GB of storage space while processing zip files, and will output a database file that is approximately 100GB. The run-time for each job is approximate 2.5 days.

#### Configuration

 The cfg.yaml files specifies target directories for the database object produced, the temporary directory where zip files are downloaded to, as well as the directory for text files that track metrics such as how many datapoints are extracted from the tab files (if > 0), which files failed to be processed, as well as output logs of the pre-processing. The filepath to the yaml file is passed to the DivinerPreprocessor object when it is initialized, which will then create the directories and database object. 

#### Preprocessing on CosmoCanyon

CosmoCanyon is a regular desktop PC running Ubuntu 22.04 LTS. A job is started by running:

	python3 data_preprocess.py /path/to/diviner-tools/config/cosmocanyon_cfg.yaml /path/to/diviner-tools/support/other/zip_urls.txt <JOBID>
Job ID is a number between 0 and 72 which represents which master batch is pre-processed.

Example job sripts can be found in jobs/cosmocanyon. 

Tip: it is recommended to start a job within a tmux session, to minimize the risk of the process  being killed if something happens to the terminal session. 

#### Preprocessing on Speed
Speed is the Concordia University High Performance Computing cluster. More information about Speed:
* https://www.concordia.ca/ginacody/aits/speed.html
* https://github.com/NAG-DevOps/speed-hpc

A conda environment should be created by submitting a specific job to do so. Subsequent preprocessing jobs will use this conda environment. See job scripts for speed in jobs/speed. 

The resulting database files can be transferred to your local machine using rsync, for example:

	
	rsync -avz --progress --partial username@speed.encs.concordia.ca:/speed-scratch/username/diviner_data/database/diviner_data_job_x.db

