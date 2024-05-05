# Diviner Tools
`diviner-tools` is a custom library that acquires Diviner data, filters and stores target data into databases, extracts area of interests datapoints and stores in database tables, and creates Profiles saved as .json files that can be used for training. 

## Requirements
* Python 3.10+
* beautifulsoup4
* numpy
* public
* PyYAML
* requests
* scikit-learn

These can be installed using: `pip install -r support/other/requirements.txt`

## Steps
### Find all .zip Files From URLs
The python script 'find_urls.py' will crawl the Diviner data host pages to find all available .zip urls. It starts at either:
* https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1001/data/ which contains data from 2009 to 2016, or
* https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1002/data/ which contains data from 2017 to the most recent release

#### All Years
To generate a text file that contains all discovered .zip file URLs from 2009 to the present, run the following: `python3 find_urls.py </path/to/output/txt/file.txt>`

This process will take approximately 10 minutes.

#### Specific Years
To generate a text file `"/path/to/file.txt"` that contains the URLs from a specific year `"YYYY"`, you can create a script:
```python
from diviner_tools import ZipCrawler, Utils

zc = ZipCrawler()
ut = Utils()

# Crawl PDS website for specific year
zip_urls = zc.findZipUrls("YYYY")

# Save results to a textfile
ut.appendToFile("/path/to/file.txt", zip_urls)
```

### Pre-process Data
The pre-processing stage involves downloading .zip files from the PDS website, unpacking the .TAB files, and then filtering out entries with target parameters. The target parameters are:

* Activity Flag (af) == 110
* Channel (c) == 7
* Emission Angle (cemis) < 10
* Calibration Quality Flag (qca) == 000
* Geometry Flag (qge) == 012
* Miscellaneous Quality Flag (qmi) == 000

The pre-processing is done in a cyclical manner involving: downloading a zip file, unpacking it, deleting the original zip file, processing the contents of the tab file, writing target datapoints to a database, then deleting the tab file. This is to minimize storage requirements.

When the pre-processing script is started, it will create several directories defined in the provided `cfg.yaml` file:
* database_directory -- the location of the database files where target data is stored
* tmp_directory -- a folder where .Zip/.TAB files are downloaded to temporarily before being deleted
* useful_tabs_directory -- a folder where text files are created that contain the URLs and number of .TABs that contained relevant data points
* bad_files_directory -- a folder where text files containing the URLs that failed to process (corrupted, unable to download, etc) for the purpose of re-processing
* log_directory -- a folder that contains logs of pre-processing status

Other relevant parameters for pre-processing include:
* transaction_size -- the size of the transaction for queries that write entries to a database
* max_workers -- the max number of threads used

See `scripts/data_preprocess.py` for an example script. If some files failed, the bad files output can be used to run a second pass on those URLs.

### Collect Area of Interest Data
After the relevant Diviner data has been acquired and stored in databases, the next step is to create an index on the database(s) to speed up queries on the `CLAT` and `CLON` columns. See `script/create_indices.py`. 

There are two configuration files that will be relevant:
* `cfg.yaml` which contains paramaters:
	* aoi_directory -- the folder where the AOI database containing tables for each AOI will be stored
	* profiles_directory -- the folder where the final Profile json files will be saved
* `aoi.yaml` which describes target areas of interests including their names, coordinates, and size

See `scripts/collect_aoi_data.py`.

After the AOI database has been generated, creating `CLON` and `CLAT` indices for each table will help speed up subsequent queries. See `scripts/create_aoi_indices.py`.

### Generate Profiles
The final step is generate the profiles. During profile generation:
* The database table containing an AOI's data points will be binned into sub-regions
* For each sub-region, `CLOCTIME` and `TB` values are extracted, and then sorted by `CLOCTIME`
* The points are decimated such that there is at least 4 hours between them
* Interpolation is applied to create 120 points for each profile
* The profile data is then saved to a json file with fields:
	* `name` of the AOI the profile belongs to
	* `boundaries` which is the upper and lower coordinates that bound the sub-region the points were extracted from
	* `data` which is 120 [`CLOCTIME`, `TB`] values

## Speed
To run this process on the Speed HPC, the scripts in `jobs/speed` can be used. Modify as required to reflect your username, directories, etc. 

To create a conda environment containing the packages required for `diviner-tools`, submit job: `sbatch scripts/speed/setup_env.sh`

The zip URLs are batched into sub-jobs 0-72. The example `support/other/zip_urls.txt` file contains 717,509 .zip URLs for the time period between 1 January 2010 to 15 September 2023, and are batched into jobs containing 10,000 URLs. A pre-processing job can be submitted with `<JOBID>` to:
```
./jobs/speed/start_job.sh <JOBID>
```
Indices on pre-processed databases can be created using:
```
sbatch jobs/speed/create_indices_job.sh
```
The AOI collection job can be started using:
```
sbatch jobs/speed/collect_aoi_data_job.sh
```
Indices can be created on the AOI database using:
```
sbatch jobs/speed/create_aoi_indices_job.sh
```
And finally, the Profiles job can be started using:
```
sbatch jobs/speed/generate_profiles_job.sh
```
To sync or download large files or directories from Speed to your local computer, `rsync` can be used. An example:
```
rsync -avz --progress --partial username@speed.encs.concordia.ca:/speed-scratch/username/diviner_data/database/diviner_data_job_x.db /path/to/destination
```
## Links
* [DLRE RDR Software Interface Specification](https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1001/document/diviner_rdr_sis.pdf)
* Concordia University Speed HPC 
	* [About Speed](https://www.concordia.ca/ginacody/aits/speed.html)
	* [Github](https://github.com/NAG-DevOps/speed-hpc)
* [LROC QuickMap](https://quickmap.lroc.asu.edu/)

