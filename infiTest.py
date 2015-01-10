#!/usr/bin/python

from wrapper import queryParallel
from wrapper import queryWrapper
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


tic = int(time.time()*1000)

query = None
p = [None] * nThreads

queries = 0

for i in range(nThreads):
	query = queryWrapper.QueryEchoprint("data/test2.json")
	query.setConnection()
	size = query.workloadSize()
	query.multiplyCodes(iterations)
	print len(query.codes)
	queries = len(query.codes)
	p[i] = queryParallel.QueryThread(query)
	p[i].setRangebyId(i, nThreads)



print "queries", queries

#print "ini"
for i in range(nThreads):
	p[i].start()

for i in range(nThreads):
	p[i].join()

total_time = int(time.time()*1000) - tic


qps = (1000.0 * queries) / total_time
print "Total time ", total_time / 1000.0
print "queries ", queries
print "QPS", qps
print "id ", cacheHits, "  ", qps


