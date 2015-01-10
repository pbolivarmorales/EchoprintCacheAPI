#!/usr/bin/python

from dbmanager import DbManager 
from query_log import QueryLog
from datetime import datetime


class AlgorithmCache(object):

	db_handler = None
	log = None
	cache_size = 100
	min_cache_size = 10
	max_cache_size = 10000
	window_frame = None 	#window frame in hours
	window_derivative = None
	new_factor = 1
	prev_date = None
	ini_date = None
	current_date = None



	def __init__(self, solarIP ="147.46.240.170", tyIP = "147.46.240.170", tyPort = 1978):
		super(AlgorithmCache, self).__init__()
		self.log = QueryLog()
		
		self.db_handler = DbManager(solarIP, tyIP, tyPort)
		self.prev_date = datetime.now()
		self.ini_date = datetime.now()
		self.prev_date = self.ini_date
		self.current_date = datetime.now()
		
		
	def set_cache_size(self, cache):
		self.cache_size = cache
		
	def setPrev(self, date):
		self.prev_date = date
		
	def setIni(self, date):
		self.ini_date = date
		
	def setCur(self, date):
		self.current_date = date
		
	def runAlgorithm(self):
		self.clear_cache()

		self.current_date = datetime.now()	
		
		rows = self.updateCache()
		self.prev_date = self.ini_date
		self.ini_date = self.current_date
		return rows
		
	def updateFrame(self):
		rows = self.updateCache()
		self.prev_date = self.ini_date
		self.ini_date = self.current_date
	
	def updateCache(self):
		rows = self.select_best_candidates()
		self.fill_cache(rows)
		print "PBM len rows ", len(rows)
		return rows
			
			
	def set_cache_size(self, size):
		self.cache_size = size
	
	def clear_cache(self):
		self.db_handler.clearDatabase()

	
	def select_best_candidates(self):
		return None
		
	def fill_cache(self, rows):
		#add songs PBM
		#self.db_handler.ingestCodesFile("../a.json")
		for row in rows:
			self.db_handler.ingestBySongID(row[0])
		
class AlgorithmCacheLRU(AlgorithmCache):
	
	
	def __init__(self, solarIP ="147.46.240.170", tyIP = "147.46.240.170", tyPort = 1978):
		super(AlgorithmCacheLRU, self).__init__(solarIP, tyIP, tyPort)
		print "LRU"
	
	def select_best_candidates(self):
		rows = self.log.selectLRU(self.ini_date, self.current_date , self.cache_size)
		return rows
		
		
class AlgorithmCacheDerivative(AlgorithmCache):
		
	def __init__(self, solarIP ="147.46.240.170", tyIP = "147.46.240.170", tyPort = 1978):
		super(AlgorithmCacheDerivative, self).__init__(solarIP, tyIP, tyPort)
		print "DER"
		
	def select_best_candidates(self):
		rows = self.log.selectDerivate(self.prev_date, self.ini_date, self.current_date , self.cache_size)
		return rows
		
		
		
class AlgorithmCacheMix(AlgorithmCache):
		
		
	perc_lru = 0.40
		
	def __init__(self, solarIP ="147.46.240.170", tyIP = "147.46.240.170", tyPort = 1978):
		super(AlgorithmCacheMix, self).__init__(solarIP, tyIP, tyPort)
		print "MIX"
		
	def set_top(self, lru):
		self.perc_lru = (1.0 * lru) / 100.0
		
	def select_best_candidates(self):
		print " Perc LRU  ", self.perc_lru
		top = int(self.cache_size * self.perc_lru)
		rest = self.cache_size - top
		rows = self.log.selectMix(self.prev_date, self.ini_date, self.current_date , top, rest)
		return rows
	
	


	
	
	
	
