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
from itertools import chain
import json
import logging
import math
import matplotlib.pyplot as plt
import numpy as np
import os
from public import public
import requests
import re
import statistics
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern
from sklearn.exceptions import ConvergenceWarning
import sqlite3
import sys
import threading
import time
import queue
from urllib.parse import urljoin
import warnings
import yaml
from zipfile import ZipFile, BadZipFile

warnings.filterwarnings("ignore")
logging.getLogger('matplotlib').setLevel(logging.ERROR)

LUNAR_RADIUS_M = 1737400.0
KM_TO_M = 1000.0

LLA = Enum("LLA", ["LAT", "LON", "ALT"], start=0)

# Data fields as per Diviner SRS
FIELD = Enum("FIELD",
             ["DATE", "UTC", "JDATE", "ORBIT", "SUNDIST",
              "SUNLAT", "SUNLON", "SCLK", "SCLAT", "SCLON",
              "SCRAD", "SCALT", "EL_CMD", "AZ_CMD", "AF",
              "ORIENTLAT", "ORIENTATION", "C", "DET", "VLOOKX",
              "VLOOKY", "VLOOKZ", "RADIANCE", "TB", "CLAT",
              "CLON", "CEMIS", "CSUNZEN", "CSUNAZI", "CLOCTIME",
              "QCA", "QGE", "QMI"],
             start=0)

# Data fields for AOIs
AOI_FIELD = Enum("AOI_FIELD",
                 ["DATETIME", "SUNDIST", "SUNLAT",
                  "SUNLON", "RADIANCE", "TB", "CLAT", "CLON",
                  "CEMIS", "CSUNZEN", "CSUNAZI", "CLOCTIME"],
                 start=1)

# Enum for area of interest classes
AOI_CLASS = Enum(
    'AOI_CLASS',
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

        # Create utils object
        self.ut = Utils()

        # Create directories if they don't exist
        self.ut.createDirectory(self.__dbDir)
        self.ut.createDirectory(self.__tmpDir)
        self.ut.createDirectory(self.__usefulTabsDir)
        self.ut.createDirectory(self.__badFilesDir)
        self.ut.createDirectory(self.__logDir)

        # Configure logger
        self.__configLogger()

        # Create database if it doesn't exist yet
        self.__createDatabase()

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
    # DATABASE MANAGEMENT
    # * * * * * * * * * * * * * * * * * * * * * * * * *

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
            # Add the url to the bad files text
            logging.error("Bad url being logged: " + repr(url))
            self.ut.appendToFile(self.__getBadFilesFilepath(), url)

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
    def createTable(self, database_path, table_name, schema):
        '''
        @brief Create table in a database
        @param database_path Path to database
        @param table_name Name of table
        @param schema SQL table def
        '''
        db_connection = None

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({schema})'
            db_cursor.execute(table_query)

            # Turn off PRAGMA synch to speed up writing
            # Note: this has a higher risk of data being corrupted
            # but hopefully since this database should only need
            # to be populated once, this is an okay risk.
            db_cursor.execute("PRAGMA synchronous = OFF;")

            db_connection.commit()

        except sqlite3.Error as e:
            logging.error("Unable to create table, will exit: " + repr(e))
            sys.exit(1)

        finally:
            if db_connection:
                db_connection.close()

    @public
    def createIndex(
            self,
            database_path,
            index_name,
            columns,
            table_name="RDR_LVL1_CH7"):
        '''
        @brief Creates an index on specified columns to speed
            up queries
        @param database_path The path to the target
                database file
        @param index_name Name of index
        @param columns A list of columns
        '''
        logging.info("Creating index {} on {} for {} in {}".format(
            index_name, columns, table_name, database_path))
        logging.info("This may take a while (20+ minutes)...")

        db_connection = None

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            index_query = "CREATE INDEX {} ON {} ({})".format(
                index_name, table_name, ", ".join(columns))
            db_cursor.execute(index_query)

            while True:
                db_cursor.execute("PRAGMA index_info('{}')".format(index_name))

                index_info = db_cursor.fetchall()

                if index_info:
                    logging.info("Done")
                    break

                time.sleep(1)

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()

    @public
    def query(self, database_path, query):
        '''
        @brief Queries target database
        @param database_path Filepath to database
        @param query SQL query
        @return Rows of matching entries
        '''
        db_connection = None
        rows = []

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            db_cursor.execute(query)

            rows = db_cursor.fetchall()

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()

        return rows

    @public
    def insert(self, database_path, queries):
        '''
        @brief Inserts data into database
        @param database_path Filepath to database
        @param queries A list of query tuples
        '''
        db_connection = None

        try:
            db_connection = sqlite3.connect(database_path)
            db_cursor = db_connection.cursor()

            for query, params in queries:
                db_cursor.execute(query, params)

            db_connection.commit()

        except sqlite3.Error as e:
            print("Error:", e)

            if db_connection:
                db_connection.rollback()
        finally:
            if db_connection:
                db_connection.close()

    @public
    def getTableList(self, database_path):
        '''
        @brief Returns a list of table names in a database
        @param database_path The path to the target database
        @return A list of table names
        '''
        db_connection = None
        tables = []

        try:
            db_connection = sqlite3.connect(database_path)

            db_cursor = db_connection.cursor()

            db_cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table';")

            tables = db_cursor.fetchall()
            tables = [table[0] for table in tables]

        except sqlite3.Error as e:
            print("Error " + repr(e))

        finally:
            if db_connection:
                db_connection.close()

        return tables


class AreaOfInterest(object):

    def __init__(self, id, name, filename, coordinates, aoi_class, size):
        '''
        @brief Data structure to hold AOI information
        @param id A numerical id value
        @param name Name of the AOI
        @param filename Filename in format {index}_{name}
        @param coordinates Lat and Lon coordinates
        @param aoi_class The type of AOI
        @param size The size of the AOI in km
        '''
        self.id = id
        self.name = name
        self.filename = filename
        self.coordinates = coordinates
        self.aoi_class = aoi_class
        self.size = size


class ProfileGenerator(object):

    def __init__(self, cfg_filepath, aoi_filepath):
        '''
        @brief Generates thermal profiles in preperation for training
        @param cfg_filepath The path to the config yaml
        @param aoi_filepath The path to the AOI data
        '''
        self.ut = Utils()
        self.dbt = DatabaseTools()

        self.__initConfig(cfg_filepath)
        self.__initAOI(aoi_filepath)
        self.__initLog(self.__logDir)

    def __initConfig(self, cfg_filepath):
        '''
        @brief Loads config params from a YAML file
        @param cfg_filepath The path to the config YAML
        '''
        # Extract generic configs
        with open(cfg_filepath, 'r') as file:
            self.__cfg = yaml.safe_load(file)

        # Pathway to databases directory
        self.__dbDir = self.__cfg["database_directory"]

        # Pathway to log directory
        self.__logDir = self.__cfg["log_directory"]

        # Max workers for threads
        self.__maxWorkers = self.__cfg["max_workers"]

        # Pathway to aoi directory
        self.__aoiDir = self.__cfg["aoi_directory"]

        # Pathway to profiles directory
        self.__profilesDir = self.__cfg["profiles_directory"]

        # Create directories if they don't exist
        self.ut.createDirectory(self.__aoiDir)
        self.ut.createDirectory(self.__profilesDir)

    def __initLog(self, log_dir):
        '''
        @brief Configures logger
        @param log_dir Path to log directory
        '''
        log_filepath = os.path.join(
            log_dir,
            "profile_generator_{0}.log".format(
                datetime.now().strftime('%Y-%m-%d_%H%M')))

        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath),
                logging.StreamHandler()])

        logging.info("Logging started: " + log_filepath)

    def __initAOI(self, aoi_filepath):
        '''
        @brief Loads AOI data
        @param aoi_filepath Path to AOI information
        '''
        self.aoi_data = self.__readAOIData(aoi_filepath)

    def __readAOIData(self, filepath):
        '''
        @brief Reads AOI data from YAML
        @param filepath Path to AOI YAML file
        '''
        with open(filepath, 'r') as file:
            data = yaml.safe_load(file)

        aoi_data = data['areas_of_interest']

        aoi_list = []

        for entry in aoi_data:
            id = entry["id"]
            name = entry["name"]
            filename = name.replace(' ', '_').replace('-', '_').upper()
            coordinates = entry["coordinates"]
            aoi_class = entry["class"]
            size = entry["size"]

            tmp = AreaOfInterest(
                id, name, filename, coordinates, aoi_class, size)

            aoi_list.append(tmp)

        return aoi_list

    @public
    def getAreasOfInterest(self):
        '''
        @brief returns AOI data in a list
        @return List containing AreaOfInterest objects
        '''
        return self.aoi_data

    @public
    def getPointBoundaries(self, target_point, distance=200):
        '''
        @brief Returns the min/max of the lat and lon
                values around a target point within distance
                Validated against: https://www.lpi.usra.edu/lunar/tools/lunardistancecalc/
        @param target_point (lat, lon)
        @param distance The desired distance between
                min and max points in meters
        @return min_lat, max_lat, min_lon, max_lon
        '''
        # For sizes expressed as an area
        if isinstance(distance, list):
            distance_lat = distance[LLA.LAT.value] / LUNAR_RADIUS_M
            distance_lon = distance[LLA.LON.value] / LUNAR_RADIUS_M

            min_lat = target_point[LLA.LAT.value] - math.degrees(distance_lat)
            max_lat = target_point[LLA.LAT.value] + math.degrees(distance_lat)

            min_lon = target_point[LLA.LON.value] - math.degrees(
                distance_lon / math.cos(math.radians(target_point[LLA.LAT.value])))
            max_lon = target_point[LLA.LON.value] + math.degrees(
                distance_lon / math.cos(math.radians(target_point[LLA.LAT.value])))

        # For sizes expressed as diameter
        else:
            distance_radians = distance / LUNAR_RADIUS_M

            min_lat = target_point[LLA.LAT.value] - \
                math.degrees(distance_radians)
            max_lat = target_point[LLA.LAT.value] + \
                math.degrees(distance_radians)
            min_lon = target_point[LLA.LON.value] - math.degrees(
                distance_radians / math.cos(math.radians(target_point[LLA.LAT.value])))
            max_lon = target_point[LLA.LON.value] + math.degrees(
                distance_radians / math.cos(math.radians(target_point[LLA.LAT.value])))

        return round(min_lat, 4), round(max_lat, 4), \
            round(min_lon, 4), round(max_lon, 4)

    def __queryTargetAOI(self, target_aoi, distance, db_path):
        '''
        @brief Queries points within a target range around
                a target area of interest coordinate
        @param target_aoi A tuple of the (lat, lon) coordinate
        @return A list of entries
        '''
        min_lat, max_lat, min_lon, max_lon = self.getPointBoundaries(
            target_aoi, distance)

        query = '''
                SELECT *
                FROM RDR_LVL1_CH7
                WHERE CLAT BETWEEN {} AND {}
                AND CLON BETWEEN {} AND {}
                '''.format(min_lat, max_lat, min_lon, max_lon)

        rows = self.dbt.query(db_path, query)

        return rows

    def __createAOITable(self, database_path, table_name):
        '''
        '''
        schema = '''
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
            '''

        self.dbt.createTable(database_path, table_name, schema)

    def __insertAOI(self, database_path, table_name, data):
        '''
        '''
        queries = []

        for row in data:
            query = f"INSERT INTO {table_name} " + \
                    "(datetime, sundist, sunlat, sunlon, " + \
                    "radiance, tb, clat, clon, cemis, " + \
                    "csunzen, csunazi, cloctime) " + \
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            queries.append((query, row))

        self.dbt.insert(database_path, queries)

    def __findTargetRows(self, target):
        '''
        @brief TODO
        @param target AreaOfInterest object
        @param number of rows found
        '''
        db_list = self.ut.getAllFilenamesFromDir(self.__dbDir)

        if isinstance(target.size, list):
            distance = [
                (target.size[LLA.LAT.value] / 2) * KM_TO_M,
                (target.size[LLA.LON.value] / 2) * KM_TO_M]
        else:
            distance = (target.size / 2) * KM_TO_M

        rows = []

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.__maxWorkers) as executor:

            futures = [executor.submit(
                self.__queryTargetAOI,
                target.coordinates,
                distance,
                os.path.join(self.__dbDir, db)) for db in db_list]

            results = [future.result()
                       for future in concurrent.futures.as_completed(futures)]

            rows = list(chain.from_iterable(results))

        if len(rows) != 0:
            db_path = os.path.join(self.__aoiDir, "aoi.db")

            # Remove col 0 which is ID
            rows = [row[1:] for row in rows]

            self.__createAOITable(db_path, target.filename)
            self.__insertAOI(db_path, target.filename, rows)

        return len(rows)

    @public
    def collectData(self):
        '''
        @brief Searches and saves AOI data
        '''
        total_entries = 0

        for target in self.aoi_data:
            num_entries = self.__findTargetRows(target)
            total_entries += num_entries

            logging.info(
                "[{0}] Total entries: {1}".format(
                    target.name, num_entries))

        logging.info("Total entries: " + repr(total_entries))
        logging.info("Done")

    def __calculateSubRegions(self, min_lat, max_lat, min_lon, max_lon, size):
        '''
        @brief Given the min/max lat/lon of a region, calculate the sub-min/max lat/lon
            values of sub-regions of a specified size
        @param min_lat Lower latitude boundary of a target region
        @param max_lat Upper latitude boundary of a target region
        @param min_lon Lower longitude boundary of a target region
        @param max_lon Upper longitude boundary of a target region
        @return A list of lists of bounding lat/lon values of subregions
        '''
        moon_circumference_m = 2 * math.pi * LUNAR_RADIUS_M
        m_per_degree = moon_circumference_m / 360
        deg_lat = size / m_per_degree

        latitudes = np.arange(min_lat, max_lat, deg_lat)
        longitudes = []

        for lat in latitudes:
            deg_lon = deg_lat / np.cos(np.radians(lat))
            longitudes.append(np.arange(min_lon, max_lon, deg_lon))

        subregions = []
        for i in range(len(latitudes) - 1):
            for j in range(len(longitudes[i]) - 1):
                subregions.append([
                    round(latitudes[i], 4),
                    round(latitudes[i + 1], 4),
                    round(longitudes[i][j], 4),
                    round(longitudes[i][j + 1], 4)])

        return subregions

    @public
    def getBinCoordinates(self, database_path, table_name, bin_size=200):
        '''
        @brief Returns a list of lists of coordinates that define
            the min/max lat/lon of sub-regions of a target whose
            data is stored within a database table
        @param table_name Name of the table containing feature data
        @param database_path Path to the database object
        @param bin_size The size of the sub-regions defined in meters
        '''
        min_lat = self.dbt.query(
            database_path,
            "SELECT MIN(CLAT) FROM {};".format(table_name))[0][0]

        max_lat = self.dbt.query(
            database_path,
            "SELECT MAX(CLAT) FROM {};".format(table_name))[0][0]

        min_lon = self.dbt.query(
            database_path,
            "SELECT MIN(CLON) FROM {};".format(table_name))[0][0]

        max_lon = self.dbt.query(
            database_path,
            "SELECT MAX(CLON) FROM {};".format(table_name))[0][0]

        return self.__calculateSubRegions(
            min_lat, max_lat, min_lon, max_lon, bin_size)

    def __filterCloctime(self, data, hours):
        '''
        @brief Given an array of data, decimate
            so that there is at least <hours>
            between data points
        @param data An array of [CLOCTIME, TB] values
        @param hours Required time between data points
        @return Filtered data points
        '''
        filtered = []

        last_cloctime = None

        for entry in data:
            cloctime = entry[0]

            if last_cloctime is None or cloctime - last_cloctime >= hours:
                filtered.append(entry)
                last_cloctime = cloctime

        return filtered

    def __interpolateGPR(self, data):
        '''
        @brief Applies Guassian Process Regression
        @param data An np array containing [CLOCTIME, TB] data
        @return An interpolated list of datapoints
        '''
        warnings.filterwarnings("ignore", category=ConvergenceWarning)

        X = data[:, 0].reshape(-1, 1)  # Time
        y = data[:, 1]  # Temperature (TB)
        
        # Enforce that the first and last temps are the same
        start_time, end_time = 0, 24
        start_temp, end_temp = y[0], y[-1]
        mean_temp = (start_temp + end_temp) / 2  
        
        X_augmented = np.vstack(([start_time], X, [end_time]))
        y_augmented = np.hstack(([mean_temp], y, [mean_temp]))
        
        # Define the GPR kernel
        kernel = 10.0 * Matern(length_scale=10.0, nu=1.5)
        
        # Fit GPR model
        gp = GaussianProcessRegressor(
            kernel=kernel, alpha=10.0, n_restarts_optimizer=10)
        gp.fit(X_augmented, y_augmented)
        
        # Predict over the new grid
        X_new = np.linspace(0, 24, 120).reshape(-1, 1)
        y_pred, _ = gp.predict(X_new, return_std=True)
        
        # Post-process to ensure the first and last predictions match
        y_pred[0] = mean_temp
        y_pred[-1] = mean_temp
        
        # Rounding
        X_new_rounded = np.around(X_new.flatten(), 5).tolist()
        y_pred_rounded = np.round(y_pred, 3).tolist()
        
        return X_new_rounded, y_pred_rounded
    
    def __verifyInterpolationInRange(self, temp_array, min_temp, max_temp):
        return min(temp_array) >= min_temp and max(temp_array) <= max_temp

    def __createProfile(self, database_path, table):
        '''
        @brief Generates a profile based on table data
        @param database_path Path to database containing table
        @param table Name of target table
        '''
        logging.info("[{}] Processing ".format(table))

        # For each AOI, create 200m x 200m sub-regions, sort
        # points by cloctime, filter, interpolate, and
        # save to json output
        subregions = self.getBinCoordinates(database_path, table)

        for index, region in enumerate(subregions):
            json_path = os.path.join(
                self.__profilesDir,
                "{}_{}.json".format(
                    table,
                    index))

            query = """
                SELECT cloctime, tb
                FROM {}
                WHERE CLAT BETWEEN {} AND {}
                AND CLON BETWEEN {} AND {}
                ORDER BY cloctime
                """.format(table, region[0], region[1], region[2], region[3])

            rows = self.dbt.query(database_path, query)
            filtered_rows = self.__filterCloctime(rows, 3)
            interpolated_time, interpolated_temps = self.__interpolateGPR(np.array(filtered_rows))

            if self.__verifyInterpolationInRange(interpolated_temps, 45, 405):
                raw_time = [row[0] for row in rows if row[1] is not None]
                raw_temp = [row[1] for row in rows if row[1] is not None]
                raw_max_temp = max(raw_temp)
                raw_min_temp = min(raw_temp)
                raw_mean_temp = statistics.mean(raw_temp)
                raw_std_temp = statistics.stdev(raw_temp)

                max_temp = max(interpolated_temps)
                min_temp = min(interpolated_temps)
                mean_temp = statistics.mean(interpolated_temps)
                std_tmp = statistics.stdev(interpolated_temps)

                profile_dict = {
                    "name": table,
                    "boundaries": region,
                    "raw_data": {
                        "time": raw_time,
                        "temps": raw_temp,
                        "statistics": {
                            "max_temp": raw_max_temp,
                            "min_temp": raw_min_temp,
                            "mean_tmp": raw_mean_temp,
                            "std_tmp": raw_std_temp
                        }
                    },
                    "interpolated_data": {
                        "time": interpolated_time,
                        "temps": interpolated_temps,
                        "statistics": {
                            "max_temp": max_temp,
                            "min_temp": min_temp,
                            "mean_tmp": mean_temp,
                            "std_tmp": std_tmp
                        }
                    }
                }

                with open(json_path, 'w') as f:
                    json.dump(profile_dict, f, indent=4)

        logging.info("[{}] Finished ".format(table))

    @public
    def generateProfiles(self):
        '''
        @brief Generates profile training data using curated
            data stored in an sqlite database, and saves the
            profiles as a .json file
        '''
        aoi_db_path = os.path.join(self.__aoiDir, "aoi.db")

        # Get list of tables in AOI database
        #table_list = self.dbt.getTableList(aoi_db_path)
        table_list = ['BANDFIELD_CRATER', 'UNNAMED_CRATER_6']
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.__maxWorkers) as executor:

            futures = [
                executor.submit(
                    self.__createProfile,
                    aoi_db_path,
                    table) for table in table_list]

            # Wait for all futures to complete
            results = [future.result()
                       for future in concurrent.futures.as_completed(futures)]
            
        logging.info("Profile generation done")

    @public
    def PlotRawAndInterpolatedData(self, json_filepath):
        with open(json_filepath, 'r') as file:
            data = json.load(file)
        
        try:
            raw_data = data["raw_data"]
            interpolated_data = data["interpolated_data"]
            
            raw_time = raw_data["time"]
            raw_temps = raw_data["temps"]
            
            interp_time = interpolated_data["time"]
            interp_temps = interpolated_data["temps"]
            
        except KeyError as e:
            print(f"Missing key in JSON data: {e}")
            return
        
        plt.figure(figsize=(12, 6))
        plt.plot(raw_time, raw_temps, 'o-', label='Raw Data', markersize=6)
        plt.plot(interp_time, interp_temps, '-', label='Interpolated Data', linewidth=1.5)
        plt.xlim(0, 24) 
        plt.ylim(0, 450) 
        
        plt.title('Comparison of Raw and Interpolated Data')
        plt.xlabel('Time (Local Lunar Time)')
        plt.ylabel('Temperature (K)')
        plt.legend()
        
        plt.grid(True)
        plt.show()


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

        # Appending a list of tuples
        elif isinstance(data, list) and isinstance(data[0], tuple):
            try:
                with open(txt_filepath, 'a') as file:
                    file.writelines('\n'.join(map(str, data)))
            except IOError as e:
                logging.error(
                    "Unable to write to file: " +
                    repr(txt_filepath) +
                    " Error message: " +
                    repr(e))

        # Appending a list of strings
        elif isinstance(data, list) and isinstance(data[0], str):
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

    @public
    def createDirectory(self, data_dir):
        '''
        @brief Creates project directory if it doesn't already exist
        @param data_dir The data directory filepath
        '''
        if not os.path.exists(data_dir):
            try:
                os.makedirs(data_dir)

            except Exception as e:
                logging.error("Unable to create directory: " + repr(data_dir))

    @public
    def createFile(self, filepath):
        '''
        @brief Creates a file if it doesn't already exist
        @param filepath The fulle name and path of target file
        '''
        if not os.path.exists(filepath):
            try:
                with open(filepath, 'w'):
                    pass

            except Exception as e:
                logging.error("Unable to create file: " + repr(filepath))
