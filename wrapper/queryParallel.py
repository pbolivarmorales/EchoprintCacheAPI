#!/usr/bin/python

from threading import Thread
from multiprocessing import Process

class QueryParallel(object):

	query = None
	iterations = 1
	size = 0
	stats = {}
	
	def __init__(self, query, ini = 0, end = 0):
		super(QueryParallel, self).__init__()
		self.query = query
		self.iterations = 1
		self.ini = ini
		self.end = end
		self.size = query.workloadSize()
		if self.end == 0:
			self.end = query.workloadSize()

		
	def setIterations(self, iterations):
		self.iterations = iterations
		
				
	def setRange(self, ini, end):
		self.ini = ini
		self.end = end
		
	def setRangebyId(self, id, num):
		rang = self.size / num
		self.ini = id * rang
		self.end = (id + 1) * rang
		if id == (num - 1):
			self.end = self.size
			
		
	def getNumQueries(self):
		return self.query.getNumQueries()
		
	def run(self):
#		print "running ", self.ini, "  ", self.end
		for i in range(self.iterations):	
			self.query.queryRange(self.ini, self.end)
			self.stats = self.query.get_stats().copy()
			if self.stats.get("cache_found"):
				print 1.0 * self.stats.get("cache_found") / self.stats.get("numQueries")

				
class QueryThread(QueryParallel, Thread):
	
	def __init__(self, query, ini = 0, end = 0):
		QueryParallel.__init__(self, query)
		Thread.__init__(self)
		
		
		
		

class QueryProcess(QueryParallel, Process):
	def __init__(self, query, ini = 0, end = 0):
		QueryParallel.__init__(self, query)
		Process.__init__(self)
		
	def run(self):
		QueryParallel.run(self)
		
		
	def getNumQueries(self):
		return QueryParallel.getNumQueries(self)


			
