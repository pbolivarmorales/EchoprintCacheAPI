#!/usr/bin/python


from wrapper import queryParallel
from wrapper import queryWrapper
import time

from CacheEngine.simulation import Simulator		



sim = Simulator("./workload.json")
sim.generate_first_epoch()

nThreads = 4
p = [None] * nThreads

def set_process(r):
	for i in range(nThreads):
		query = queryWrapper.QueryEchoprint()
		query.setConnection()
		query.setCodes(r)
		p[i] = queryParallel.QueryProcess(query)
		p[i].setRangebyId(i, nThreads)


for epoch in range(10):
	print "epoch ", epoch
	r = sim.get_workload()
	for i in range(nThreads):
		query = queryWrapper.QueryEchoprint()
		query.setConnection()
		query.setCodes(r)
		p[i] = queryParallel.QueryProcess(query)
		p[i].setRangebyId(i, nThreads)

	tic = int(time.time()*1000)


	for i in range(nThreads):
		p[i].start()

	for i in range(nThreads):
		p[i].join()


	total_time = int(time.time()*1000) - tic

	#queries = 600 * iterations
	queries = len(r)

	qps = (1000.0 * queries) / total_time
	print "Total time ", total_time / 1000.0
	print "queries ", queries
	print "QPS", qps

	print "--------------------------\n"
	sim.generate_next_epoch()





