'''
@file diviner_tools.py
@author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)

@brief Tools to help pre-process diviner data:
	- DivinerPreprocessor
	- DatabaseTools
	- ProfileGenerator
	- ZipCrawler
	- Utils
'''
from bs4 import BeautifulSoup
import concurrent.futures
from datetime import datetime
from enum import Enum
import logging
import math
import os
from public import public
import requests
import re
import sqlite3
import sys
import threading
import time
import queue
from urllib.parse import urljoin
import yaml
from zipfile import ZipFile, BadZipFile

LUNAR_RADIUS_M = 1737400.0

# Enum for data fields
FIELD = Enum("FIELD",
             ["DATE", "UTC", "JDATE", "ORBIT", "SUNDIST",
              "SUNLAT", "SUNLON", "SCLK", "SCLAT", "SCLON",
              "SCRAD", "SCALT", "EL_CMD", "AZ_CMD", "AF",
              "ORIENTLAT", "ORIENTATION", "C", "DET", "VLOOKX",
              "VLOOKY", "VLOOKZ", "RADIANCE", "TB", "CLAT",
              "CLON", "CEMIS", "CSUNZEN", "CSUNAZI", "CLOCTIME",
              "QCA", "QGE", "QMI"],
             start=0)

# Enum for area of interest classes
AOI = Enum(
    'AOI',
    ['THERMAL', 'MICROWAVE', 'MAGNETIC', 'BACKGROUND'],
    start=1)


class DivinerPreprocessor(object):
    '''
    @brief A class to pre-process Diviner RDR LVL1 CH7 Data
    '''

    def __init__(self, cfg_filepath, label=""):

        # Job label
        self.__label = label

        # Extract configs
        with open(cfg_filepath, 'r') as file:
            self.__cfg = yaml.safe_load(file)

        # Pathway to database directory
        self.__dbDir = self.__cfg['database_directory']

        # Pathway to tmp directory
        self.__tmpDir = self.__cfg['tmp_directory']

        # Pathway to useful tabs directory
        self.__usefulTabsDir = self.__cfg['useful_tabs_directory']

        # Pathway to bad files directory
        self.__badFilesDir = self.__cfg['bad_files_directory']

        # Pathway to logs directory
        self.__logDir = self.__cfg['log_directory']

        # Max transaction size for SQL queries
        self.__transactionSize = self.__cfg['transaction_size']

        # Max number of thread workers
        self.__maxWorkers = self.__cfg['max_workers']

        # Create directories if they don't exist
        self.__createDir(self.__dbDir)
        self.__createDir(self.__tmpDir)
        self.__createDir(self.__usefulTabsDir)
        self.__createDir(self.__badFilesDir)
        self.__createDir(self.__logDir)

        # Configure logger
        self.__configLogger()

        # Create database if it doesn't exist yet
        self.__createDatabase()

        # Create utils object
        self.ut = Utils()

    # * * * * * * * * * * * * * * * * * * * * * * * * *
    # FILEPATHS
    # * * * * * * * * * * * * * * * * * * * * * * * * *

    def __getDatabaseFilepath(self):
        '''
        @brief Returns database filepath

        @return A string to the database file
        '''
        return os.path.join(
            self.__dbDir,
            "diviner_data_" + self.__label + ".db")

    def __getUsefulTabsFilepath(self):
        '''
        @brief Returns database filepath

        @return A string to the database file
        '''
        return os.path.join(
            self.__usefulTabsDir,
            "useful_tabs_" + self.__label + ".txt")

    def __getBadFilesFilepath(self):
        '''
        @brief Returns database filepath

        @return A string to the database file
        '''
        return os.path.join(
            self.__badFilesDir,
            "bad_files_" + self.__label + ".txt")

    def __getLogFilepath(self):
        '''
        @brief Returns database filepath

        @return A string to the database file
        '''
        return os.path.join(
            self.__logDir,
            "diviner_tools_" + self.__label + "_" +
            datetime.now().strftime('%Y-%m-%d_%H%M') + ".log")

    # * * * * * * * * * * * * * * * * * * * * * * * * *
    # JOB QUEUE
    # * * * * * * * * * * * * * * * * * * * * * * * * *

    def __jobMonitor(self):
        '''
        @brief 	Monitors a job queue for data that needs to be written
                to a database.
        '''
        # To speed up writing, we can process
        # jobs in 'chunks'
        jobs_chunk = []

        while True:
            curr_qsize = self.job_queue.qsize()

            if (curr_qsize > 0):

                job = self.job_queue.get()

                if job is None:
                    logging.info("Job monitor stopped")
                    break

                else:
                    jobs_chunk.append(job)

                    # Dynamically determine the chunk size
                    curr_chunk_size = min(curr_qsize, self.__transactionSize)

                    # Fill chunk list
                    for j in range(curr_chunk_size - 1):
                        jobs_chunk.append(self.job_queue.get())

                    # Send chunk to be processed
                    self.__writeChunk(jobs_chunk)

                    # Clear chunk
                    jobs_chunk = []

    def __startJobMonitor(self):
        '''
        @brief Starts a thread that runs an SQL job monitor
        '''
        self.job_queue = queue.Queue()

        self.job_monitor = threading.Thread(target=self.__jobMonitor)
        self.job_monitor.start()

    def __stopJobMonitor(self):
        '''
        @brief	Waits until all SQL jobs are complete and then issues
                a stop command that will stop the job monitor thread
        '''
        # Wait until all jobs are completed
        self.__waitForJobQueueToEmpty()

        # Send None job to stop job monitor
        self.job_queue.put(None)

    def __waitForJobQueueToEmpty(self):
        '''
        @brief Waits for the job queue to empty
        '''
        while (self.job_queue.qsize() > 0):
            logging.info("Remaining jobs: {}".format(self.job_queue.qsize()))
            time.sleep(1)

    def __writeChunk(self, jobs_chunk):
        '''
        @brief Writes a chunk of queued jobs to SQL database

        @param jobs_chunk A list containing a chunk of jobs
        '''
        db_connection = None

        try:
            db_connection = sqlite3.connect(self.__getDatabaseFilepath())
            db_cursor = db_connection.cursor()

            for job in jobs_chunk:
                db_cursor.execute('''
					  INSERT INTO RDR_LVL1_CH7 (
					  datetime, sundist, sunlat, sunlon, radiance, tb,
					  clat, clon, cemis, csunzen, csunazi, cloctime
					  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
					  ''',
                                  job)

            db_connection.commit()

        except sqlite3.Error as e:
            logging.error("Unable to write job to database: " + repr(e) +
                          " - Job contents: " + repr(job))

        finally:
            if db_connection:
                db_connection.close()

    # * * * * * * * * * * * * * * * * * * * * * * * * *
    # TIMING AND LOGGING
    # * * * * * * * * * * * * * * * * * * * * * * * * *

    def __configLogger(self):
        '''
        @brief Configures logger module
        '''
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.__getLogFilepath()),
                logging.StreamHandler()])

        logging.info("Logging started: " + repr(self.__getLogFilepath()))

    # * * * * * * * * * * * * * * * * * * * * * * * * *
    # FILE/DIRECTORY MANAGEMENT
    # * * * * * * * * * * * * * * * * * * * * * * * * *

    def __createDir(self, data_dir):
        '''
        @brief Creates project directory if it doesn't already exist

        @param data_dir The data directory filepath
        '''
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)

            except Exception as e:
                logging.error("Unable to create directory: " + repr(data_dir))

    def __createDatabase(self):
        '''
        @brief Create database if it doesn't exist
        '''
        db_connection = None

        try:
            db_connection = sqlite3.connect(self.__getDatabaseFilepath())

            db_cursor = db_connection.cursor()

            # Creating the table schema based on the RDR SIS
            rdr_lvl1_ch7_schema = '''
				CREATE TABLE IF NOT EXISTS RDR_LVL1_CH7 (
				id INTEGER PRIMARY KEY,
				datetime TEXT,
				sundist REAL,
				sunlat REAL,
				sunlon REAL,
				radiance REAL,
				tb REAL,
				clat REAL,
				clon REAL,
				cemis REAL,
				csunzen REAL,
				csunazi REAL,
				cloctime REAL
				);
			'''

            db_cursor.execute(rdr_lvl1_ch7_schema)

            # Turn off PRAGMA synch to speed up writing
            # Note: this has a higher risk of data being corrupted
            # but hopefully since this database should only need
            # to be populated once, this is an okay risk.
            db_cursor.execute("PRAGMA synchronous = OFF;")

            db_connection.commit()

        except sqlite3.Error as e:
            logging.error("Unable to create database, will exit: " + repr(e))
            sys.exit(1)

        finally:
            if db_connection:
                db_connection.close()

    def __getTab(self, dest_dir, src_url):
        '''
        @brief	Given a link to a .zip file, this function will
                download, unpack the .zip file, then delete
                the original .zip file to minimize storage used

        @param local_dir The local directory to save to
        @param zip_url The url to the target .zip file
        '''
        ok = True

        # Extract filename
        filename = os.path.join(dest_dir, src_url.split("/")[-1])

        # Download the zip file
        try:
            response = requests.get(src_url)

            # Check status of request
            response.raise_for_status()

            with open(filename, 'wb') as file:
                file.write(response.content)

        # Something goes wrong with http request
        except requests.exceptions.RequestException as e:
            logging.error("Unable to make request to url: " + repr(src_url) +
                          " Error message: " + repr(e))
            ok = False

        # Something goes wrong with writing to file
        except IOError as e:
            logging.error("Unable to write to file: " + repr(filename) +
                          " Error message: " + repr(e))
            ok = False

        # Something else went wrong
        except Exception as e:
            logging.error(
                "An error occured when trying to acquire .zip file: " +
                repr(filename))

        # Extract the contents of the zip file
        if os.path.exists(filename):
            try:
                with ZipFile(filename, 'r') as zip_ref:
                    zip_ref.extractall(dest_dir)

            # Something goes wrong with opening the zip file
            except BadZipFile as e:
                logging.error("Unable to open unzip file: " + repr(filename) +
                              " Error message: " + repr(e))
                ok = False

        # The file does not exist
        else:
            logging.error("Zip file " + repr(filename) + "does not exist")
            ok = False

        # Delete original .zip file
        try:
            os.remove(filename)

        # Unable to delete the zip file, although not deleting
        # is okay, so we won't set ok = False for this failing
        except Exception as e:
            logging.error("Unable to delete zip file: " + repr(filename) +
                          " Error message: " + repr(e))

        return ok

    # * * * * * * * * * * * * * * * * * * * * * * * * *
    # PREPROCESSING
    # * * * * * * * * * * * * * * * * * * * * * * * * *

    def __checkParams(self, data):
        '''
        @brief Checks if an RDR table entry matches filter criteria

        @param data RDR table entry

        @return A boolean on whether or not the data meets criteria
        '''
        # Check the data conforms to the params:
        #    af == 110
        #    c == 7
        #    cemis < 10
        #    qca == 0
        #    qge == 12
        #    qmi == 0
        if (data[FIELD.AF.value] == "110") and (data[FIELD.C.value] == "7") and \
                (float(data[FIELD.CEMIS.value]) < 10.0) and (data[FIELD.QCA.value] == "000") and \
                (data[FIELD.QGE.value] == "012") and (data[FIELD.QMI.value] == "000"):

            return True

        else:
            return False

    def __processLine(self, data):
        '''
        @brief Checks if line is valid before adding to job queue

        @param data The text line containing the data entry
        @param 0 or 1 depending if the data was added or not
        '''
        # Split the line
        values = data.strip().split(',')

        # Remove any whitespaces and extra quotations from the values
        values = [val.strip().strip('"') for val in values]

        # Check that the data conforms to desired params
        dataok = self.__checkParams(values)

        if (dataok):
            try:
                # Join DATE and UTC fields to create datetime
                date_time = "{0} {1}".format(
                    values[FIELD.DATE.value], values[FIELD.UTC.value])

                # The specific values to be inserted
                job_values = [date_time,
                              float(values[FIELD.SUNDIST.value]),
                              float(values[FIELD.SUNLAT.value]),
                              float(values[FIELD.SUNLON.value]),
                              float(values[FIELD.RADIANCE.value]),
                              float(values[FIELD.TB.value]),
                              float(values[FIELD.CLAT.value]),
                              float(values[FIELD.CLON.value]),
                              float(values[FIELD.CEMIS.value]),
                              float(values[FIELD.CSUNZEN.value]),
                              float(values[FIELD.CSUNAZI.value]),
                              float(values[FIELD.CLOCTIME.value])]

                # Adding job to job queue
                self.job_queue.put(job_values)

                return 1

            except Exception as e:
                logging.error("Error adding SQL job to queue: ", e)
                return 0
        else:
            return 0

    def __processor(self, url):
        '''
        @brief	Preprocesses RDR data tables all the way from download
                to writing to the database

        @param url The url of the .zip file containing RDR data
        '''
        tabok = self.__getTab(self.__tmpDir, url)

        # Synth filename from url
        file = re.search(r'(\d{12}_rdr)', url)[0].upper()
        filename = os.path.join(self.__tmpDir, file + ".TAB")

        if (tabok):
            # Read lines from .TAB file
            lines = self.ut.tabToLines(filename)

            # Process each line and add to database it qualifies
            # Note: if we use multithreading, sqlite3 doesn't
            # support concurrent writes, so a job monitor is needed
            count = 0

            for line in lines:
                count += self.__processLine(line)

            # Since there are files that contain no target
            # data, we want to keep track of the ones that do
            # so that in the future we don't have to download
            # every RDR file if we need to redo preprocessing
            if (count > 0):
                data = url + " " + repr(count)
                self.ut.appendToFile(
                    self.__getUsefulTabsFilepath(), data)
                logging.info("Added " + repr(count) + " files to job queue")

            # We no longer need the .TAB data and will delete
            # it to preserve storage space
            try:
                os.remove(filename)

            except Exception as e:
                logging.error("Unable to delete TAB file: " + repr(filename) +
                              " Error message: " + repr(e))

        else:
            # Add the filename to the bad files text
            logging.error("Bad file being logged: " + repr(filename))
            self.ut.appendToFile(
                self.__getBadFilesFilepath(), filename)

    @public
    def preprocess(self, data):
        '''
        @brief Initiates the pre-processing loop

        @param data A list of zip urls
        '''
        # Log start time
        start_t = self.ut.timeStamp()
        logging.info("Start time: " + self.ut.timeToString(start_t))

        # Start the SQL job monitor
        self.__startJobMonitor()

        # Split data into batches based on max workers
        batched_data = self.ut.batch(data, self.__maxWorkers)

        for n, batch in enumerate(batched_data, start=0):

            # Log current status
            status = "Processing batch {0} of {1} - {2}% - Time elapsed: {3}".format(
                n + 1,
                len(batched_data),
                round(((n + 1) / len(batched_data)) * 100),
                self.ut.elapsedTime(start_t))

            logging.info(status)

            # Start thread pool, should choose max workers carefully to not
            # overrun memory
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.__maxWorkers) as executor:

                futures = [
                    executor.submit(
                        self.__processor,
                        url) for url in batch]

                # Wait for all futures to complete
                results = [future.result()
                           for future in concurrent.futures.as_completed(futures)]

                # Wait for job queue to empty before starting next batch
                self.__waitForJobQueueToEmpty()

        # Stop the job monitor (the job monitor will wait for the queue to
        # empty first)
        self.__stopJobMonitor()

        # Time elapsed
        delta_t = self.ut.elapsedTime(start_t)
        logging.info("Total elapsed time: " + delta_t)

        # Entries added to database
        num_entries = self.ut.countEntries(self.__getUsefulTabsFilepath())
        logging.info(
            "Number of entries added to database: " +
            repr(num_entries))

        # Number of bad files
        num_bad = self.ut.countLines(self.__getBadFilesFilepath())
        logging.info("Number of bad files: " + repr(num_bad))


class DatabaseTools(object):
    '''
    @brief Tools for managing sqlite databases
    '''

    def __init__(self):
        pass

    @public
    def createCoordinateIndex(self, database_path):
        '''
        @brief Creates an index on CLAT and CLON that
                speeds up coordinate-based queries
        @param database_path The path to the target
                database file
        '''
        print("Creating index on CLAT, CLON for " +
              database_path)

        print("This may take a while (20+ minutes)...")

        db_connection = None

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            db_cursor.execute('''
                CREATE INDEX idx_clat_clon
                ON RDR_LVL1_CH7 (CLAT, CLON);
                ''')

            while True:
                db_cursor.execute("PRAGMA index_info('idx_clat_clon')")

                index_info = db_cursor.fetchall()

                if index_info:
                    print("Done")
                    break

                time.sleep(1)

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()

    def createDatetimeIndex(self, database_path):
        '''
        @brief Creates an index on Datetime that
                speeds up datetime-based queries
        @param database_path The path to the target
                database file
        '''

        print("Creating index on DATETIME for " +
              database_path)

        print("This may take a while (20+ minutes)...")

        db_connection = None

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            db_cursor.execute('''
                CREATE INDEX idx_datetime
                ON RDR_LVL1_CH7 (DATETIME);
                ''')

            while True:
                db_cursor.execute("PRAGMA index_info('idx_datetime')")

                index_info = db_cursor.fetchall()

                if index_info:
                    print("Done")
                    break

                time.sleep(1)

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()


class ProfileGenerator(object):
    '''
    @brief Generates thermal profiles in preperation for training
    '''

    def __init__(self):
        pass

    def getAOICoordinateList(self, filepath):
        '''
        @brief Returns a list of AOI coordinates
        @param filepath The path to the yaml file containing
                AOI data
        @return a list of coordinate tuples
        '''
        with open(filepath, 'r') as file:
            data = yaml.safe_load(file)

        coordinates = [(entry['coordinates'][0], entry['coordinates'][1])
                       for entry in data['areas_of_interest']]

        return coordinates

    def queryDatetimeRange(self, database_path, start_datetime, end_datetime):
        '''
        @brief Returns entries between dates
        @param database_path Filepath to database object
        @param start_datetime Start time in format DD-MMM-YYYY HH:mm:SS:sss
        @param start_datetime End time in format DD-MMM-YYYY HH:mm:SS:sss
        @return A list of entries
        '''
        db_connection = None
        rows = list()

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            db_cursor.execute('''
					 SELECT *
					 FROM RDR_LVL1_CH7
					 WHERE DATETIME BETWEEN ? AND ?
					 ORDER BY DATETIME DESC;
					 ''',
                              (start_datetime, end_datetime))

            rows = db_cursor.fetchall()

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()

        return rows

    def getPointBoundaries(self, target_point, distance=200):
        '''
        @brief Returns the min/max of the lat and lon
                values around a target point within distance
                Validated against: https://www.lpi.usra.edu/lunar/tools/lunardistancecalc/
        @param target_point (lat, lon)
        @param distance The desired distance between
                min and max points
        @return min_lat, max_lat, min_lon, max_lon
        '''
        distance_radians = distance / LUNAR_RADIUS_M

        min_lat = target_point[0] - math.degrees(distance_radians)
        max_lat = target_point[0] + math.degrees(distance_radians)
        min_lon = target_point[1] - math.degrees(
            distance_radians / math.cos(math.radians(target_point[0])))
        max_lon = target_point[1] + math.degrees(
            distance_radians / math.cos(math.radians(target_point[0])))

        return round(min_lat, 4), round(max_lat, 4), \
            round(min_lon, 4), round(max_lon, 4)

    def queryTargetAOI(self, target_aoi):
        '''
        @brief Queries points within a target range around
                a target area of interest coordinate
        @param target_aoi A tuple of the (lat, lon) coordinate
        @return A list of entries
        '''
        min_lat, max_lat, min_lon, max_lon = self.getPointBoundaries(
            target_aoi)

        db_connection = None
        rows = list()

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            db_cursor.execute("""
                SELECT *
                FROM RDR_LVL1_CH7
                WHERE CLAT BETWEEN ? AND ?
                AND CLON BETWEEN ? AND ?
                """,
                              (min_lat, max_lat, min_lon, max_lon))

            rows = db_cursor.fetchall()

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()

        return rows


class ZipCrawler(object):
    '''
    @brief A class to find .zip files containing Diviner data
    '''

    def __init__(self):
        pass

    def __getSubUrls(self, parent_url, pattern=None):
        '''
        @brief Returns a list of sub-links on a parent page

        @param parent_url The url page that is being searched
        @param pattern A regex pattern if required to filter the url list

        @return A list of sub-links on the page
        '''
        try:
            # Send a GET request to get page elements
            response = requests.get(parent_url)

            # Check request status
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Extract sub-urls
            sub_urls = [urljoin(parent_url, link.get("href"))
                        for link in soup.find_all("a", href=True)]

            # Filter the list using regex if a pattern is specified
            if pattern:
                sub_urls = [
                    url for url in sub_urls if re.compile(pattern).match(url)]

            return sub_urls

        except requests.exceptions.RequestException as e:
            logging.error("Unable to access url: " + repr(parent_url) +
                          " Error message: " + repr(e))

            # Return empty list
            return []

    def __crawl(self, input_urls, pattern=None):
        '''
        @brief Crawls through urls on a page using multithreading

        @param input_urls The parent urls to search
        @param pattern Optional regex pattern to match url against
        '''
        # Use multi-threading
        with concurrent.futures.ThreadPoolExecutor() as executor:

            if pattern:
                target_urls_list = list(
                    executor.map(
                        lambda target: self.__getSubUrls(
                            target, pattern), input_urls))
            else:
                target_urls_list = list(
                    executor.map(
                        lambda target: self.__getSubUrls(
                            target, target), input_urls))

        # Collapse into single list
        target_urls = [url for sublist in target_urls_list for url in sublist]

        return target_urls

    @public
    def findZipUrls(self, target_year=None):
        '''
        @brief Walks through the RDR V1 parent links to find all zip file urls
        '''
        # lroldr_1001 contains data from 2009 - 2016
        # lroldr_1002 contains data from 2017 - 2023
        parent_urls = [
            'https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1001/data/',
            'https://pds-geosciences.wustl.edu/lro/lro-l-dlre-4-rdr-v1/lrodlr_1002/data/']

        # Regex pattern will filter URLs for years
        # If a year is specified, the search is only for that year
        # Otherwise it is for all years 2010-2023
        if target_year:
            pattern = r'.*/{0}/'.format(target_year)
        else:
            pattern = r'.*/20[1-2]\d/$'

        # Generate list of year urls
        year_urls = self.__getSubUrls(parent_urls[0], pattern) + \
            self.__getSubUrls(parent_urls[1], pattern)

        # Search for month urls
        month_urls = self.__crawl(year_urls)

        # Search for day urls
        day_urls = self.__crawl(month_urls)

        # Search for zip urls
        zip_urls = self.__crawl(day_urls, r'.+\.zip$')

        return zip_urls


class Utils(object):
    '''
    @brief A utilities class with helper functions
    '''

    def __init__(self):
        pass

    @public
    def tabToLines(self, src_tab):
        '''
        @brief Parses .TAB file into lines

        @param src_tab Source .TAB file

        @return A list of strings
        '''
        lines = []

        try:
            # Open and read .TAB file starting at line 5
            with open(src_tab, 'r') as file:
                for _ in range(4):
                    next(file)

                # Read each line and remove carriage character
                for line in file:
                    lines.append(line.rstrip('^M'))

        except IOError as e:
            logging.error("Unable to read file: " + repr(src_tab) +
                          " Error message: " + repr(e))

        return lines

    @public
    def txtToList(self, txt_filepath):
        '''
        @brief Generates a list from a textfile

        @param txt_filepath The path to the target text file
        '''
        try:
            with open(txt_filepath, 'r') as file:
                lines = [line.strip() for line in file.readlines()]

            return lines

        except IOError as e:
            logging.error("Unable to open file: " + repr(txt_filepath) +
                          " Error message: " + repr(e))

            # Return an empty list
            return []

    @public
    def appendToFile(self, txt_filepath, data):
        '''
        @brief Appends data to target text file

        @param txt_filepath The text file path
        @param data The data to be appended
        '''
        # Appending a string
        if isinstance(data, str):
            try:
                with open(txt_filepath, 'a') as file:
                    file.write(data + '\n')
            except IOError as e:
                logging.error(
                    "Unable to write to file: " +
                    repr(txt_filepath) +
                    " Error message: " +
                    repr(e))

        # Appending a list of strings
        elif isinstance(data, list):
            try:
                with open(txt_filepath, 'a') as file:
                    file.writelines('\n'.join(data))
            except IOError as e:
                logging.error(
                    "Unable to write to file: " +
                    repr(txt_filepath) +
                    " Error message: " +
                    repr(e))

    @public
    def batch(self, input_list, batch_size):
        '''
        @brief Splits a list into a list of lists of a specified size

        @param input_list A list
        @param batch_size The desired size of sub-lists

        @return A list of lists
        '''
        return [input_list[i:i + batch_size]
                for i in range(0, len(input_list), batch_size)]

    @public
    def timeToString(self, time_t):
        return time_t.strftime('%Y-%m-%d %H:%M')

    @public
    def timeStamp(self):
        '''
        @brief Returns current time

        @return Time stamp
        '''
        return datetime.now()

    @public
    def elapsedTime(self, start_time):
        '''
        @brief	Determines elapsed time given a start time and prints
                in human-readable format

        @param start_time The start time

        @return Time elapsed in string DD:HH:mm:ss
        '''
        end_t = self.timeStamp()

        # Total elapsed time
        delta_t = end_t - start_time

        # Calculate total seconds in the timedelta
        total_seconds = int(delta_t.total_seconds())

        # Extract days, hours, minutes, and seconds
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Format the output as DD:HH:mm:ss
        formatted_time_delta = f"{days:02}:{hours:02}:{minutes:02}:{seconds:02}"

        return formatted_time_delta

    @public
    def countEntries(self, filepath):
        '''
        @brief Returns the number of entries recorded in
                the useful tabs file

        @param filepath The path to the useful tabs file

        @return The number of entries recorded
        '''
        count = 0

        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as file:
                    for line in file:
                        parts = line.split()
                        number = int(parts[-1])
                        count += number

            except Exception as e:
                logging.error(
                    "Unable to open useful tabs file: " +
                    repr(filepath) +
                    " Error message: " +
                    repr(e))

        return count

    @public
    def countLines(self, filepath):
        '''
        @brief Counts the number of lines in a file

        @param filepath The path to the target file

        @return The number of lines
        '''
        count = 0

        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as file:
                    for line in file:
                        count += 1

            except Exception as e:
                logging.error(
                    "Unable to open filepath to count lines: " +
                    repr(filepath) +
                    " Error message: " +
                    repr(e))

        return count

    @public
    def getAllFilenamesFromDir(self, dir):
        '''
        @brief Returns a list of filenames

        @param dir The directory path

        @return A list of filenames
        '''
        filenames = []

        for filename in os.listdir(dir):
            if os.path.isfile(os.path.join(dir, filename)):
                filenames.append(filename)

        return filenames
