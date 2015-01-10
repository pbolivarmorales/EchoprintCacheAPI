#!/usr/bin/python

import sys
from wrapper import queryWrapper
import time
from datetime import datetime

from CacheEngine.simulation import Simulator2		
from CacheEngine import cachealgorithm


cache = None
if sys.argv[1] == "lru":
	cache = cachealgorithm.AlgorithmCacheLRU()
elif sys.argv[1] == "der":
	cache = cachealgorithm.AlgorithmCacheDerivative()
else:
	print "error argv[2]"
	sys.exit(1)

global_stats = {}
sim = Simulator2("../test2.json")
cache.clear_cache()
sim.generate_first_epoch()
total_tic = int(time.time()*1000)

for epoch in range(4):
	
	tic = int(time.time()*1000)
	t = datetime.now()
	print "epoch ", epoch
	r = sim.get_workload()

	print "queries ", len(r)

	query = queryWrapper.QueryEchoprintCache()
	query.setConnection()
	query.setConnectionCache()
	query.setCodes(r)


	t_cache = int(time.time()*1000)
	cache.runAlgorithm()
	t_cache_end = int(time.time()*1000) - t_cache
	
	
	query.query()

	epoch_time = int(time.time()*1000) - tic
	
	if (len(global_stats) == 0):
		global_stats = query.get_stats().copy()
	else:
		stats = query.get_stats()
		for k in global_stats:
			global_stats[k] += stats[k]

	queries = len(r)

	print "time ", epoch_time/ 1000.0
	print "cache time ", t_cache_end / 1000.0
	print "queries ", queries
	print "stats ", query.get_stats()
	print "global stats ", global_stats
	print "hits: ", 100.0 * query.get_stats()["cache_found"] / query.get_stats()["numQueries"]
	print "global hits: ", 100.0 * global_stats["cache_found"] / global_stats["numQueries"]
	

	print "--------------------------\n"
	sim.generate_next_epoch()
	
total_time = int(time.time()*1000) - tic
print "Total time ", total_time/ 1000.0

