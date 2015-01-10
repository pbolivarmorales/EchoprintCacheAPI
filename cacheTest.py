#!/usr/bin/python

import sys
from wrapper import queryWrapper
from wrapper import queryParallel
import time
from datetime import datetime
	
from CacheEngine import cachealgorithm

import logging
logging.basicConfig()


cache = None
if sys.argv[1] == "lru":
	cache = cachealgorithm.AlgorithmCacheLRU()
elif sys.argv[1] == "der":
	cache = cachealgorithm.AlgorithmCacheDerivative()
elif sys.argv[1] == "mix":
	cache = cachealgorithm.AlgorithmCacheMix()
else:
	print "error argv[1]"
	sys.exit (1)


cache_size = int(sys.argv[2])
cache.set_cache_size(cache_size)


cache.clear_cache()
total_tic = int(time.time()*1000)
global_queries = 0
global_time = 0

     
cache.setPrev("2014-11-26 13:01:03.455931")
cache.setIni("2014-11-26 13:01:03.455931")
cache.setCur(" 2014-11-26 13:01:07.602232")




for epoch in range(1):
	
	tic = int(time.time()*1000)
	print "cache ", cache_size
	
	cache.runAlgorithm()

		
	#sim.get_stats_sim()
		
	t_cache_end = int(time.time()*1000) - tic


	print "cache time ", t_cache_end / 1000.0


	print "--------------------------\n"
	
total_time = int(time.time()*1000) - tic
print "Total time ", total_time/ 1000.0


