#!/usr/bin/python

import sys
from wrapper import queryWrapper
from wrapper import queryParallel
import time
from datetime import datetime

from CacheEngine.simulation import Simulator2		
	
from CacheEngine import cachealgorithm

import logging
logging.basicConfig()


nThreads = int(sys.argv[1])
p = [None] * nThreads


n_epoch = 3
cache = None
if sys.argv[2] == "lru":
	cache = cachealgorithm.AlgorithmCacheLRU()
	n_epoch = 2
elif sys.argv[2] == "der":
	cache = cachealgorithm.AlgorithmCacheDerivative()
elif sys.argv[2] == "mix":
	cache = cachealgorithm.AlgorithmCacheMix()
	if sys.argv[5]:
		cache.set_top(int(sys.argv[5]))
else:
	print "error argv[2]"
	sys.exit (1)
	
cache_size = int(sys.argv[3])
epoch_elem = int(sys.argv[4])


cache.set_cache_size(cache_size)

global_stats = {}
sim = Simulator2("../test2.json", epoch_elem)
sim.set_epoch_elem(epoch_elem)
cache.clear_cache()
sim.generate_first_epoch()
total_tic = int(time.time()*1000)
global_queries = 0
global_time = 0




sim.get_der()
sim.get_stats_sim()


for epoch in range(n_epoch):
	
	tic = int(time.time()*1000)
	t = datetime.now()
	print "epoch ", epoch
	print "aasdfasdf"
	r = sim.get_workload()
	queries = len(r)
	print "queries ", queries
	
	for i in range(nThreads):
		query = queryWrapper.QueryEchoprintCache("workload.json")
		query = queryWrapper.QueryEchoprintCache()
		query.setConnection()
		query.setConnectionCache()
		query.setCodes(r)
		size = query.workloadSize()
		p[i] = queryParallel.QueryProcess(query)
		p[i].setRangebyId(i, nThreads)

	t_cache = int(time.time()*1000)

	cache.runAlgorithm()

		
	#sim.get_stats_sim()
		
	t_cache_end = int(time.time()*1000) - t_cache

	#print "ini"
	for i in range(nThreads):
		p[i].start()

	for i in range(nThreads):
		p[i].join()

	epoch_time = int(time.time()*1000) - tic
	

	
	real_time = epoch_time - t_cache_end
	global_queries += queries
	global_time += real_time

	qps = (1000.0 * queries) / real_time
	global_qps = (1000.0 * global_queries) / global_time
	print "date", datetime.now()
	print "cache time ", t_cache_end / 1000.0
	print "time ", epoch_time/ 1000.0
	print "QPS", qps
	print "Global QPS", global_qps

	print "--------------------------\n"
	sim.generate_next_epoch()
	
total_time = int(time.time()*1000) - tic
print "Total time ", total_time/ 1000.0


