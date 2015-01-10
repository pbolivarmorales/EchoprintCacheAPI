#!/usr/bin/python

import sys
from API import fp2
import json
import time
from random import shuffle
import copy
from CacheEngine.query_log import QueryLog
from datetime import datetime

def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))

class QueryEchoprint(object):

	defaultProt = 1978
	infi1Solr = "http://147.46.240.168:8502/solr/fp"
	infi1Tyrant = "147.46.240.168"

	ultraSolr = "http://147.46.240.170:8502/solr/fp"
	ultraTyrant = "147.46.240.170"
	

	
	def __init__(self, workloadFile = None):
		super(QueryEchoprint, self).__init__()
		self.workloadFile = workloadFile
		
		self.initialize()
		
		if workloadFile:
			with open(self.workloadFile) as data_file:
				self.codes = json.load(data_file)

		shuffle(self.codes)
		
		
	def initialize(self):
		self.codes = []
	
		self.log = QueryLog()
		self.db = None
		self.log_buffer = []
		self.log_b_size = 10000

		self.total_time = 0
		self.total_query_time = 0

		self.found = 0
		self.notFound = 0
		self.threadTime = 0
	
		self.ok = 0
		self.notCorrect = 0
	
		self.numQueries = 0
	
		
	def __del__(self):
		if self.log:
			self.log.commit()
			
	def setCodes(self, cods, shuffle = False):
		self.codes = cods
		if shuffle:
			shuffle(self.codes)
		
	def setConnection(self, solr = infi1Solr, tokyo = infi1Tyrant, tokyoPort = defaultProt):
		self.db = fp2.FpConnection(solr, tokyo, tokyoPort)
		
	def workloadSize(self):
		return len(self.codes)

		
	def multiplyCodes(self, number):
		codesCopy = copy.copy(self.codes)
		for i in range(number-1):
			self.codes.extend(codesCopy)
		shuffle(self.codes)
	
	def reset(self):
		self.total_time = 0
		self.total_query_time = 0
		self.found = 0
		self.notFound = 0
		self.threadTime = 0	
		self.numQueries = 0
		
	def getNumQueries(self):
		return self.numQueries
		
	def querySong(self, song):
		r = self.db.best_match_for_query(song["code"])

		self.total_query_time += r.qtime
		self.total_time += r.total_time

		if r.match():
			self.found += 1
			trackID = r.metadata["track_id"].split("-")
			self.updateLog(trackID[0], 0, r.qtime, r.total_time, datetime.now())

			if song["metadata"]["title"] == r.metadata.get("track"):
				self.ok +=1
			else:
				self.notCorrect +=1			
		else:
			self.notFound += 1
		
		return r
			
			

	def updateLog(self, trackID, hit, qtime, total_time, time_now):
		row = [trackID, hit, qtime, total_time, time_now]
		self.log_buffer.append(row)
		size = len(self.log_buffer)
		if(size > self.log_b_size):
			self.writeLog()
			
	def writeLog(self):
		#print "write"
		for r in self.log_buffer:
			self.log.insertLog(r[0], r[1], r[2], r[3], r[4])
		self.log.commit()
		self.log_buffer = []

	def query(self):
		for song in self.codes:
			self.querySong(song)
			self.numQueries += 1
			
		self.writeLog()
				
	def queryRange(self, ini, end):
		for i in range(ini, end):
			song = self.codes[i]
			self.querySong(song)
			self.numQueries += 1
			
		self.writeLog()
			
	def get_stats(self):
		self.dict = {}
		self.dict["found"] = self.found
		self.dict["notFound"] = self.notFound
		self.dict["notCorrect"] = self.notCorrect
		self.dict["correct"] = self.ok
		self.dict["numQueries"] = self.numQueries
		self.dict["time"] = self.total_time
		self.dict["qtime"] = self.total_query_time
		return self.dict
				
	def printTime(self):
		print "found\tnot-found\ttotal\tquery-time\t time"
		print self.found, "\t\t", self.notFound, "\t\t", self.found + self.notFound, "\t\t", self.total_query_time, "\t\t", self.total_time
		print "correct\tNot-correct\tfalse-positive"
		print self.ok, "\t\t", self.notCorrect, "\t\t", self.found - self.ok, "\n\n"

		
class QueryEchoprintCache(QueryEchoprint):
	

	
	def __init__(self, workloadFile = None):
		super(QueryEchoprintCache, self).__init__(workloadFile)
		self.initialize()
		
	def initialize(self):
		super(QueryEchoprintCache, self).initialize()
		
		self.localdb = None
		
		self.foundCache = 0
		self.notFoundCache = 0
	
		self.okCache = 0
		self.notCorrectCache = 0

	def setConnectionCache(self, solr = QueryEchoprint.ultraSolr, tokyo = QueryEchoprint.ultraTyrant, tokyoPort = QueryEchoprint.defaultProt):
		self.localdb = fp2.FpConnection(solr, tokyo, tokyoPort)

	
	def querySong(self, song):
		elbow = 100
		if song.get("code"):
			r = self.localdb.best_match_for_query_cache(song["code"], elbow, False)
		else:
			print song
			
		self.total_query_time += r.qtime
		self.total_time += r.total_time

		if r.match():
			self.foundCache += 1
			trackID = r.metadata["track_id"].split("-")
			self.updateLog(trackID[0], 1, r.qtime, r.total_time, datetime.now())
			
			if song["metadata"]["title"] == r.metadata.get("track"):
				self.okCache +=1
				self.ok +=1
			else:
				self.notCorrectCache +=1
				self.notCorrect +=1

			
		else:
			self.notFoundCache += 1
			super(QueryEchoprintCache, self).querySong(song)
		
	
	def reset(self):
		super(QueryEchoprintCache, self).reset()
		self.foundCache = 0
		self.notFoundCache = 0
		
	def get_stats(self):
		self.dict = super(QueryEchoprintCache, self).get_stats()
		self.dict["cache_found"] = self.foundCache
		self.dict["cache_notFound"] = self.notFoundCache
		self.dict["cache_notCorrect"] = self.notCorrectCache
		self.dict["cache_correct"] = self.okCache
		return self.dict
		
	def printTime(self):
		print "found-cache\tnot-found-cache\ttotal-cache"
		print self.foundCache, "\t\t", self.notFoundCache, "\t\t", self.foundCache + self.notFoundCache
		print "correct-cache\tNot-correct-cache\tfalse-positive-cache"
		print self.okCache, "\t\t", self.notCorrectCache, "\t\t", self.foundCache - self.okCache
		super(QueryEchoprintCache, self).printTime()
	

