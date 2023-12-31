#!/usr/bin/env python3
"""! @brief Helper object with functions for pre-processing Diviner data"""

##
# @file diviner_tools.py
# @author Chantelle G. Dubois (chantelle.dubois@mail.concordia.ca)
#	
from bs4 import BeautifulSoup
import concurrent.futures
from enum import Enum
import os
import requests
import re
import sqlite3
import sys
import threading
import time
import queue
from urllib.parse import urljoin
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

	def __init__(self, data_dir, db_filepath):

		self.__createDir(data_dir)
		self.__createDatabase(db_filepath)

	
	def __writeChunk(self, jobs_chunk, db_filepath):
		
		db_connection = sqlite3.connect(db_filepath)
		db_cursor = db_connection.cursor()

		for job in jobs_chunk:
			db_cursor.execute(JOB_TEMPLATE, job)

		db_connection.commit()
		db_connection.close()


	def __dbJobProcessor(self, db_filepath):

		# To speed up writing, we can process
		# jobs in 'chunks'
		jobs_chunk = []

		MAX_CHUNK_SIZE = 1000

		while True:
			curr_qsize = self.job_queue.qsize()

			if (curr_qsize > 0):

				job = self.job_queue.get()

				if job is None:
					print("Job monitor stopped")
					break

				else:
					jobs_chunk.append(job)

					# Dynamically determine the chunk size
					curr_chunk_size = min(curr_qsize, MAX_CHUNK_SIZE)

					# Fill chunk list 
					for j in range(curr_chunk_size - 1):
						jobs_chunk.append(self.job_queue.get())

					# Send chunk to be processed
					self.__writeChunk(jobs_chunk, db_filepath)


	def startDatabaseJobMonitor(self, db_filepath):
		# Database write jobs queue
		self.job_queue = queue.Queue()

		self.job_monitor = threading.Thread(target=self.__dbJobProcessor, args=(db_filepath,))
		self.job_monitor.start()


	def stopDatabaseJobMonitor(self):

		# Wait until all jobs are completed
		while (self.job_queue.qsize() > 0):

			# Trying to enforce printing on the same line...
			sys.stdout.write("\033[K") 
			sys.stdout.write("\rThere are {} jobs left".format(self.job_queue.qsize()))
			sys.stdout.flush()

			time.sleep(1)

		# Flush the line one last time
		sys.stdout.flush()

		# Send None job to stop job monitor
		self.job_queue.put(None)


	def __createDir(self, data_dir):
		"""! Creates project directory if it doesn't already exist

		@param data_dir The data directory filepath
		"""
		if not os.path.exists(data_dir):
			os.makedirs(data_dir)


	def __createDatabase(self, db_filepath):
		"""! Create database if it doesn't exist

		@param db_filepath The filepath to the database object
		"""
		db_connection = sqlite3.connect(db_filepath)

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

		# Turn of PRAGMA synch to speed up writing
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


	def insert_into_database(self, dest_db, data):
		"""! Adds a Diviner RDR LVL1 data line into a target database

		@param dest_db The pathway to the destination database
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


	def append_to_file(self, filepath, data):
		"""! Appends data to target text file

		@param filepath The text file path
		@param data The data to be appended
		"""
		# Appending a string
		if isinstance(data, str):
			with open(filepath, 'a') as file:
				file.write(data + '\n')

		# Appending a list of strings
		elif isinstance(data, list):
			with open(filepath, 'a') as file:
				file.writelines('\n'.join(data))


	def txt_to_list(self, filepath):
		"""! Generates a list from a textfile

		@param filepath The path to the target text file
		"""
		with open(filepath, 'r') as file:
			lines = [line.strip() for line in file.readlines()]

		return lines

	def processor(self, url, tab_dir, db_filepath, useful_tab_file):
		"""! Preprocesses RDR data tables all the way from download
			to writing to the database

		@param url The url of the .zip file containing RDR data
		@param tab_dir The directory to save the files
		@param db_filepath The filepath to the database
		@param tab_txt_filepath The filepath of a text file to 
			keep track of useful filenames
		"""
		self.download_unpack_delete(tab_dir, url)

		# Synth filename from url
		file = re.search(r'(\d{12}_rdr)', url)[0].upper()
		filename = os.path.join(tab_dir,  file + ".TAB")

		# Read lines from .TAB file
		lines = self.tab_to_lines(filename)

		# Process each line and add to database it qualifies
		# Note: if we use multithreading, sqlite3 doesn't
		# support concurrent writes, but it's supposed to
		# somehow queue write requests
		count = 0
    
		for line in lines:
			count += self.insert_into_database(db_filepath, line)

		# Since there are files that contain no target 
		# data, we want to keep track of the ones that do
		# so that in the future we don't have to download
		# every RDR file
		if (count > 0):
			data = file + " " + repr(count)
			self.append_to_file(useful_tab_file, data)

		# We no longer need the .TAB data and will delete
		# it to preserve storage space
		os.remove(filename)

