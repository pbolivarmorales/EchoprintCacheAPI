#!/usr/bin/python

import json
import random

class Simulator(object):


	pop = [1, 37, 100, 60, 5, 2, 1, 1]
	semipop = [1, 10, 25, 45, 42, 1, 1, 1]
	rock = [1, 2, 3, 25, 40, 10, 4, 1]
	normal = [1, 1, 1, 1, 1, 1, 1, 1]
	
	pop_freq = 0.25
	semipop_freq = 0.15
	rock_freq = 0.20
	normal_freq = 0.40
	
	patters = {"pop" : [pop, pop_freq], "semipop" : [semipop, semipop_freq], "rock" : [rock, rock_freq], "normal" : [normal, normal_freq]}
	
	epoch = 0
	index = 0
	
	codes = None
	workloadFile = None
	state = None
	
	workload = []
	
	
	def __init__(self, workloadFile, elem = 200):
		super(Simulator, self).__init__()
		self.ini_songs = elem
		self.epoch_new_songs = elem
		self.workloadFile = workloadFile
		with open(self.workloadFile) as data_file:
			self.codes = json.load(data_file)
		self.num_sogs = len(self.codes)
		
		random.shuffle(self.codes, random.random)
		
		self.index = 0
		self.epoch = 0
		self.n_epoch = len(self.pop)
		
		
	

	def set_epoch_elem(self, elem):
		self.ini_songs = elem
		self.epoch_new_songs = elem
		print "aaaaaaaaaaaaaaa", self.epoch_new_songs

	
	def generate_first_epoch(self):
		for k in self.patters:
			n_songs = int(self.patters[k][1] * self.ini_songs)
			next = self.index + n_songs
			for i in range(self.index, next):
				map = {}
				map["t"] = 0
				map["kind"] = k
				map["size"] = self.patters[k][0][0]
				map["id"] = i % self.num_sogs
				self.workload.append(map)
				
			self.index = next
			
	def generate_next_epoch(self):
		self.epoch += 1

		for song in self.workload:
			song["t"] += 1
			song["size"] = self.patters[song["kind"]][0][song["t"] % self.n_epoch]
		self.add_song_epoch()
			
			
	def add_song_epoch(self):
		for k in self.patters:
			n_songs = int(self.patters[k][1] * self.epoch_new_songs)
			next = self.index + n_songs
			for i in range(self.index, next):
				map = {}
				map["t"] = 0
				map["kind"] = k
				map["size"] = self.patters[k][0][0]
				map["id"] = i % self.num_sogs
				self.workload.append(map)
				
			self.index = next
		
	def get_workload(self):
		ret = []
		for song in self.workload:
			for i in range(song["size"]):
				ret.append(self.codes[song["id"]])
 
		random.shuffle(ret, random.random)

		return ret
		
		
	def get_der(self):
		res = []
		for key in self.patters:
			row = []
			list = self.patters[key][0]
			
			row.append(key)
			size = len(list)
			for i in range(size):
				der = list[i-1] - list[i-2]
				val = der + list[i-1]
				row.append(val)
			res.append(row)
			
		print('\n'.join([''.join(['{0:8}'.format(item) for item in row]) for row in res]))
		print "------------------------------------------------------------------------------"
		
	def get_stats_sim(self):
		res = []
		song = []
		for key in self.patters:
			row = []
			row_song = []
			value = self.patters[key]
			freq  = value[1]
			
			ele = int(self.epoch_new_songs * freq)
			
			row.append(key)
			row_song.append(key)
			for cell in value[0]:
				row.append(cell * ele)
				row_song.append(ele)
			res.append(row)
			song.append(row_song)
			
		print('\n'.join([''.join(['{0:8}'.format(item) for item in row]) for row in res]))
		print "------------------------------------------------------------------------------"
		print('\n'.join([''.join(['{0:8}'.format(item) for item in row]) for row in song]))
		print "------------------------------------------------------------------------------"

		return res
			
		
		

class Simulator2(Simulator):

	def __init__(self, workloadFile, elem = 200):
		super(Simulator2, self).__init__(workloadFile, elem)
		#self.codes = self.codes[0:500]
		#self.num_sogs = len(self.codes)
		#print self.num_sogs

		super(Simulator2, self).generate_first_epoch()
		for i in range(self.n_epoch - 1):
			super(Simulator2, self).generate_next_epoch()
		
	def generate_first_epoch(self):
		None
			
	def generate_next_epoch(self):
		self.epoch += 1

		for song in self.workload:
			song["t"] += 1
			song["size"] = self.patters[song["kind"]][0][song["t"] % self.n_epoch]
		#self.add_song_epoch()
			
			

		
