#!/usr/bin/python

import queryParallel
import queryWrapper
import time
import sys


def dump(obj):
   for attr in dir(obj):
       if hasattr( obj, attr ):
           print( "obj.%s = %s" % (attr, getattr(obj, attr)))


if len (sys.argv) != 4 :
    print "Usage: python qps_nuevo.py nThreads iterations cache_hits"
    sys.exit (1)
    
nThreads = int(sys.argv[1])
iterations = int(sys.argv[2])
cacheHits = sys.argv[3]

p = [None] * nThreads

for i in range(nThreads):
	query = queryWrapper.QueryEchoprintCache("workload.json")
	query.setConnection()
	size = query.workloadSize()
	query.setConnectionCache()
	query.multiplyCodes(iterations)
	p[i] = queryParallel.QueryProcess(query)
	p[i].setRangebyId(i, nThreads)




tic = int(time.time()*1000)

#print "ini"
for i in range(nThreads):
	p[i].start()

for i in range(nThreads):
	p[i].join()

total_time = int(time.time()*1000) - tic

queries = 600 * iterations
for i in range(nThreads):
	queries += p[i].getNumQueries()

qps = (1000.0 * queries) / total_time
print "Total time ", total_time / 1000.0
print "queries ", queries
print "QPS", qps
print "id ", cacheHits, "  ", qps


