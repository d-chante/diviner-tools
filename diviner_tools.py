##
# @file diviner_tools.py
# @author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)
#	

from bs4 import BeautifulSoup
import concurrent.futures
from datetime import datetime, timedelta
from enum import Enum
import os
import pytz
import requests
import re
import sqlite3
import sys
import threading
from tqdm import tqdm
import time
import queue
from urllib.parse import urljoin
import yaml
from zipfile import ZipFile


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

# The SQL job template
JOB_TEMPLATE = '''
	INSERT INTO RDR_LVL1_CH7 (
		date, utc, jdate, orbit, sundist, sunlat, sunlon, sclk, sclat, sclon,
		scrad, scalt, el_cmd, az_cmd, af, vert_lat, vert_lon, c, det, vlookx,
		vlooky, vlookz, radiance, tb, clat, clon, cemis, csunzen, csunazi,
		cloctime, qca, qge, qmi
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	'''


class DivinerTools(object):

	def __init__(self, cfg_filepath):

		# Extract configs
		with open(cfg_filepath, 'r') as file:
			self.__cfg = yaml.safe_load(file)

		# Filepath to database
		self.__dbFilepath = self.__cfg['database_filepath']

		# Pathway to tmp directory
		self.__tmpDir = self.__cfg['tmp_directory']

		# Filepath to the useful tab files list
		self.__usefulTabsFilepath = self.__cfg['useful_tabs_filepath']

		# Batch size for number of .TAB files processed per iteration
		self.__batchSize = self.__cfg['batch_size']

		# Flag to indicate whether or not to use timer
		self.__useTimer = self.__cfg['use_timer'] 
		
		# Create database if it doesn't exist yet
		self.__createDatabase()

	
	def __writeChunk(self, jobs_chunk):
		"""! Writes a chunk of queued jobs to SQL database
		
		@param jobs_chunk A list containing a chunk of jobs
		"""
		
		db_connection = sqlite3.connect(self.__dbFilepath)
		db_cursor = db_connection.cursor()

		for job in jobs_chunk:
			db_cursor.execute(JOB_TEMPLATE, job)

		db_connection.commit()
		db_connection.close()


	def __jobMonitor(self):
		"""! Monitors a job queue for data that needs to be written
			to a database.
		"""

		# To speed up writing, we can process
		# jobs in 'chunks'
		jobs_chunk = []

		MAX_CHUNK_SIZE = 1000

		while True:
			curr_qsize = self.job_queue.qsize()

			if (curr_qsize > 0):

				job = self.job_queue.get()

				if job is None:
					print("\nJob monitor stopped")
					break

				else:
					jobs_chunk.append(job)

					# Dynamically determine the chunk size
					curr_chunk_size = min(curr_qsize, MAX_CHUNK_SIZE)

					# Fill chunk list 
					for j in range(curr_chunk_size - 1):
						jobs_chunk.append(self.job_queue.get())

					# Send chunk to be processed
					self.__writeChunk(jobs_chunk)


	def __startJobMonitor(self):
		"""! Starts a thread that runs an SQL job monitor
		"""
		self.job_queue = queue.Queue()

		self.job_monitor = threading.Thread(target=self.__jobMonitor)
		self.job_monitor.start()


	def __stopJobMonitor(self):
		"""! Waits until all SQL jobs are complete and then issues
			a stop command that will stop the job monitor thread
		"""

		# Wait until all jobs are completed
		while (self.job_queue.qsize() > 0):

			# Trying to enforce printing on the same line...
			sys.stdout.write("\033[K") 
			sys.stdout.write("\rThere are {} jobs left".format(self.job_queue.qsize()))
			sys.stdout.flush()

			time.sleep(1)

		# One last flush
		sys.stdout.flush()
		
		# Send None job to stop job monitor
		self.job_queue.put(None)


	def __createDir(self, data_dir):
		"""! Creates project directory if it doesn't already exist

		@param data_dir The data directory filepath
		"""
		if not os.path.exists(data_dir):
			os.makedirs(data_dir)


	def __createDatabase(self):
		"""! Create database if it doesn't exist
		"""
		db_connection = sqlite3.connect(self.__dbFilepath)

		# Creating a cursor object allows us to interact
		# with the database object through SQL commands
		db_cursor = db_connection.cursor()

		# Creating the table schema based on the RDR SIS
		rdr_lvl1_ch7_schema = '''
			CREATE TABLE IF NOT EXISTS RDR_LVL1_CH7 (
			id INTEGER PRIMARY KEY,
			date TEXT,
			utc TEXT,
			jdate REAL,
			orbit INTEGER,
			sundist REAL,
			sunlat REAL,
			sunlon REAL,
			sclk REAL,
			sclat REAL,
			sclon REAL,
			scrad REAL,
			scalt REAL,
			el_cmd REAL,
			az_cmd REAL,
			af INTEGER,
			vert_lat REAL,
			vert_lon REAL,
			c INTEGER,
			det INTEGER,
			vlookx REAL,
			vlooky REAL,
			vlookz REAL,
			radiance REAL,
			tb REAL,
			clat REAL,
			clon REAL,
			cemis REAL,
			csunzen REAL,
			csunazi REAL,
			cloctime REAL,
			qca INTEGER,
			qge INTEGER,
			qmi INTEGER
			);
		'''

		# Execute the SQL to define the table schema
		db_cursor.execute(rdr_lvl1_ch7_schema)

		# Turn off PRAGMA synch to speed up writing
		# Note: this has a higher risk of data being corrupted
		# but hopefully since this database should only need
		# to be populated once, this is an okay risk.
		db_cursor.execute("PRAGMA synchronous = OFF;")

		# Then commit and close the connection
		db_connection.commit()
		db_connection.close()


	def get_sub_urls(self, parent_url, pattern=None):
		"""! Returns a list of sub-links on a parent page

		@param parent_url The url page that is being searched
		@param pattern A regex pattern if required to filter the url list
		@return A list of sub-links on the page
		"""

		# Send a GET request to get page elements
		response = requests.get(parent_url)
		soup = BeautifulSoup(response.text, "html.parser")

		# Extract sub-urls
		sub_urls = [urljoin(parent_url, link.get("href")) for link in soup.find_all("a", href=True)]

		# Filter the list using regex if a pattern is specified
		if pattern:
			sub_urls = [url for url in sub_urls if re.compile(pattern).match(url)]

		return sub_urls
	

	def multithread_crawl(self, input_urls, pattern=None):
		"""! Crawls through urls on a page using multithreading

		@param input_urls The parent urls to search
		@param pattern Optional regex pattern to match url against 
		"""
	
		# Use multi-threading
		with concurrent.futures.ThreadPoolExecutor() as executor:
	
			if pattern:
				target_urls_list = list(executor.map(lambda target: self.get_sub_urls(target, pattern), input_urls))
			else:
				target_urls_list = list(executor.map(lambda target: self.get_sub_urls(target, target), input_urls))

		# Collapse into single list
		target_urls = [url for sublist in target_urls_list for url in sublist] 

		return target_urls
	
	
	def find_all_zip_urls(self, target_year=None):
		"""! Walks through the RDR V1 parent links to find all zip file urls"""

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
		year_urls = self.get_sub_urls(parent_urls[0], pattern) + self.get_sub_urls(parent_urls[1], pattern)
	
		# Search for month urls
		month_urls = self.multithread_crawl(year_urls)

		# Search for day urls
		day_urls = self.multithread_crawl(month_urls)

		# Search for zip urls
		zip_urls = self.multithread_crawl(day_urls, r'.+\.zip$')

		return zip_urls
	

	def download_unpack_delete(self, dest_dir, src_url):
		"""! Given a link to a .zip file, this function will
			download, unpack the .zip file, then delete
			the original .zip file to minimize storage used
	
		@param local_dir The local directory to save to
		@param zip_url The url to the target .zip file
		"""

		# Verify the destination directory exists
		os.makedirs(dest_dir, exist_ok=True)

		# Extract filename
		filename = os.path.join(dest_dir, src_url.split("/")[-1])

		# Download the zip file
		response = requests.get(src_url)
		with open(filename, 'wb') as file:
			file.write(response.content)

		# Extract the contents of the zip file
		with ZipFile(filename, 'r') as zip_ref:
			zip_ref.extractall(dest_dir)

		# Delete original .zip file
		os.remove(filename)


	def check_params(self, data):
		"""! Checks if an RDR table entry matches filter criteria

		@param data RDR table entry

		@return A boolean on whether or not the data meets criteria
		"""

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


	def insert_into_database(self, data):
		"""! Adds a Diviner RDR LVL1 data line into a target database

		@param data The text line containing the data entry
		@param 0 or 1 depending if the data was added or not
		"""
	
		# Split the line    
		values = data.strip().split(',')

		# Remove any whitespaces from the values
		values = [val.strip() for val in values]

		# Check that the data conforms to desired params
		dataok = self.check_params(values)
	
		if(dataok):
			try:
				# The specific values to be inserted
				job_values = [
					values[FIELD.DATE.value], values[FIELD.UTC.value], float(values[FIELD.JDATE.value]),
					float(values[FIELD.ORBIT.value]), float(values[FIELD.SUNDIST.value]), float(values[FIELD.SUNLAT.value]),
					float(values[FIELD.SUNLON.value]), float(values[FIELD.SCLK.value]), values[FIELD.SCLAT.value],
					float(values[FIELD.SCLON.value]), float(values[FIELD.SCRAD.value]), float(values[FIELD.SCALT.value]),
					float(values[FIELD.EL_CMD.value]), float(values[FIELD.AZ_CMD.value]), float(values[FIELD.AF.value]),
					float(values[FIELD.ORIENTLAT.value]), float(values[FIELD.ORIENTATION.value]), float(values[FIELD.C.value]),
					int(values[FIELD.DET.value]), float(values[FIELD.VLOOKX.value]), float(values[FIELD.VLOOKY.value]),
					float(values[FIELD.VLOOKZ.value]), float(values[FIELD.RADIANCE.value]), float(values[FIELD.TB.value]),
					float(values[FIELD.CLAT.value]), float(values[FIELD.CLON.value]), float(values[FIELD.CEMIS.value]),
					float(values[FIELD.CSUNZEN.value]), float(values[FIELD.CSUNAZI.value]), float(values[FIELD.CLOCTIME.value]),
					float(values[FIELD.QCA.value]), int(values[FIELD.QGE.value]), int(values[FIELD.QMI.value])]

				# Adding job to job queue
				self.job_queue.put(job_values)
				
				return 1

			except Exception as e:
				print("Error adding SQL job to queue: ", e)
				return 0
		else:
			return 0
	

	def tab_to_lines(self, src_tab):
		"""! Parses .TAB file into lines

		@param src_tab Source .TAB file
		@return A list of strings
		"""

		lines = []

		# Open and read .TAB file starting at line 5
		with open(src_tab, 'r') as file:
			for _ in range(4):
				next(file)

			# Read each line and remove carriage character
			for line in file:
				lines.append(line.rstrip('^M'))

		return lines


	def append_to_file(self, txt_filepath, data):
		"""! Appends data to target text file

		@param txt_filepath The text file path
		@param data The data to be appended
		"""
		# Appending a string
		if isinstance(data, str):
			with open(txt_filepath, 'a') as file:
				file.write(data + '\n')

		# Appending a list of strings
		elif isinstance(data, list):
			with open(txt_filepath, 'a') as file:
				file.writelines('\n'.join(data))


	def txt_to_list(self, txt_filepath):
		"""! Generates a list from a textfile

		@param txt_filepath The path to the target text file
		"""
		with open(txt_filepath, 'r') as file:
			lines = [line.strip() for line in file.readlines()]

		return lines


	def __startTimer(self):
		"""! Logs a start time
		"""

		# Log start time
		start_t = datetime.now(pytz.timezone('America/Montreal'))

		print("\nStart time: " + repr(start_t.strftime('%Y-%m-%d %H:%M')))

		return start_t

	def __timeElapsed(self, start_time):
		"""! Determines elapsed time given a start time and prints
			in human-readable format

		@param start_time The start time
		"""

		end_t = datetime.now(pytz.timezone('America/Montreal'))
		print("\nEnd time: " + repr(end_t.strftime('%Y-%m-%d %H:%M')))

		# Total elapsed time
		delta_t = end_t - start_time

		# Calculate total seconds in the timedelta
		total_seconds = int(delta_t.total_seconds())

		# Extract hours, minutes, and seconds
		hours, remainder = divmod(total_seconds, 3600)
		minutes, seconds = divmod(remainder, 60)

		# Format the output as HH:mm:ss
		formatted_time = f"{hours:02}:{minutes:02}:{seconds:02}"

		print("\nElapsed time: " + formatted_time)
	

	def __processor(self, url):
		"""! Preprocesses RDR data tables all the way from download
			to writing to the database

		@param url The url of the .zip file containing RDR data
		"""
		self.download_unpack_delete(self.__tmpDir, url)

		# Synth filename from url
		file = re.search(r'(\d{12}_rdr)', url)[0].upper()
		filename = os.path.join(self.__tmpDir,  file + ".TAB")

		# Read lines from .TAB file
		lines = self.tab_to_lines(filename)

		# Process each line and add to database it qualifies
		# Note: if we use multithreading, sqlite3 doesn't
		# support concurrent writes, so a job monitor is needed
		count = 0
    
		for line in lines:
			count += self.insert_into_database(line)

		# Since there are files that contain no target 
		# data, we want to keep track of the ones that do
		# so that in the future we don't have to download
		# every RDR file if we need to redo preprocessing
		if (count > 0):
			data = url + " " + repr(count)
			self.append_to_file(self.__usefulTabsFilepath, data)

		# We no longer need the .TAB data and will delete
		# it to preserve storage space
		os.remove(filename)

	def batch(self, input_list, batch_size):
		"""! Splits a list into a list of lists of a specified size

		@param input_list A list
		@param batch_size The desired size of sub-lists
		"""
		return [input_list[i:i + batch_size] for i in range(0, len(input_list), batch_size)]


	def preprocess(self, data):
		"""! Initiates the pre-processing loop

		@param data A list of zip urls
		"""

		# Start timer if the timer option is selected
		if (self.__useTimer):
			start_t = self.__startTimer()

		# Split data into batches
		batched_data = self.batch(data, self.__batchSize)

		# Start the SQL job monitor 
		self.__startJobMonitor()

		for n, batch in tqdm(enumerate(batched_data, start=0), total=len(batched_data)):

			print("=========== Batch: " + repr(n) + " ===========")

			# Start thread pool, using 10 workers max to avoid overrunning
			# memory or the job queue
			with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

				futures = [executor.submit(self.__processor, url) for url in batch]

				# Wait for all futures to complete 
				results = [future.result() for future in concurrent.futures.as_completed(futures)]

		# Stop the job monitor
		self.__stopJobMonitor()

		# End timer if the timer option is selected
		if (self.__useTimer):
			self.__timeElapsed(start_t)
		