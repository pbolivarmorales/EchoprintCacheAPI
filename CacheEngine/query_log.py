#!/usr/bin/python

import sqlite3
from datetime import datetime
from datetime import timedelta
from os.path import expanduser
home = expanduser("~")
db_path = home + "/example.db"

class QueryLog:

	create_table_log = '''create table if not exists QueryLog (
							id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
							songID TEXT, 
							cacheHIT INTEGER,
							queryTime INTEGER,
							totalTime INTEGER,
							Timestamp DATETIME)'''
							
	create_songid_index = '''CREATE INDEX if not exists
								QueryLog_songID_idx ON QueryLog (songID)'''

	create_table_ref = '''create table if not exists QueryRef (
							id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
							songID TEXT, 
							code TEXT,
							metadata TEXT)'''
							
	create_songid_index2 = '''CREATE INDEX if not exists
								QueryRef_songID_idx ON QueryRef (songID)'''
							
	insert_log = """INSERT INTO QueryLog(songID, cacheHIT, queryTime, totalTime, Timestamp) 
				VALUES(?, ?, ?, ?, ?)"""

	insert_ref = """INSERT INTO QueryRef(songID, code, metadata) 
				VALUES(?, ?, ?)"""

	select_queryref = """SELECT songID, code, metadata
				FROM QueryRef
				WHERE SongID = ?"""
				
	select_lru = """SELECT songID, count(*) as count 
					FROM QueryLog 
					WHERE DATETIME(timestamp) BETWEEN ? AND ?
					GROUP BY songID 
					ORDER BY count DESC 
					LIMIT ?"""
					
	select_derive = """SELECT  L1.SONGID, L1.COUNT, 2 * L1.COUNT - COALESCE(L2.COUNT, 0) AS DER, 
						L1.COUNT - L2.COUNT AS DIF
					FROM ( 
						(SELECT A.SONGID, COUNT(A.SONGID) AS COUNT
						FROM QUERYLOG AS A
						WHERE DATETIME(timestamp) 
							BETWEEN ? AND ?
						GROUP BY A.SONGID) AS L1 LEFT OUTER JOIN
						(SELECT A.SONGID, COUNT(A.SONGID) AS COUNT
						FROM QUERYLOG AS A
						WHERE DATETIME(timestamp) BETWEEN ? AND ?
						GROUP BY A.SONGID) AS L2
						ON L1.SONGID = L2.SONGID)
					ORDER BY DER DESC
					LIMIT ?"""
	  
	  		
					
	select_mix = """SELECT  L1.SONGID, L1.COUNT, 2 * L1.COUNT - COALESCE(L2.COUNT, 0) AS DER, 
						L1.COUNT - L2.COUNT AS DIF
					FROM ( 
						(SELECT A.SONGID, COUNT(A.SONGID) AS COUNT
						FROM QUERYLOG AS A
						WHERE DATETIME(timestamp) 
							BETWEEN ? AND ?
						GROUP BY A.SONGID) AS L1 LEFT OUTER JOIN
						(SELECT A.SONGID, COUNT(A.SONGID) AS COUNT
						FROM QUERYLOG AS A
						WHERE DATETIME(timestamp) BETWEEN ? AND ?
						GROUP BY A.SONGID) AS L2
						ON L1.SONGID = L2.SONGID)
					WHERE L1.SONGID  NOT IN (
						SELECT E.songID
						FROM QueryLog E
						WHERE DATETIME(timestamp) BETWEEN ? AND ?
						GROUP BY songID 
						ORDER BY COUNT(*) DESC 
						LIMIT ?
					)
					ORDER BY DER DESC
					LIMIT ?"""
					
	
	def __init__(self, path = db_path):
		self.conn = sqlite3.connect(path, timeout = 40.0, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self.c = self.conn.cursor()
		self.c.execute(self.create_table_log)
		self.c.execute(self.create_songid_index)
		self.c.execute(self.create_table_ref)
		self.c.execute(self.create_songid_index2)
		self.conn.commit()
		self.c.close()
		
		
	def commit(self):
		self.conn.commit()
		
	def insertLog(self, songID, cacheHit, queryTime, totalTime, time_now = datetime.now()):
		self.c = self.conn.cursor()
		self.c.execute(self.insert_log, (songID, cacheHit, queryTime, totalTime, time_now))
		self.c.close()

	def insertRef(self, songID, code, metadata):
		self.c = self.conn.cursor()
		self.c.execute(self.insert_ref, (songID, code, metadata))
		self.c.close()

	def selectRef(self, songID):
		self.c = self.conn.cursor()
		self.c.execute(self.select_queryref, (songID,))
		row = self.c.fetchone()
		self.c.close()
		return row
		
		
	def selectDerivate(self, prev_date, ini_date, end_date, rows):
		self.c = self.conn.cursor()
		print prev_date, " ", ini_date, " ", end_date
		self.c.execute(self.select_derive, (ini_date, end_date, prev_date, ini_date, rows))
		rows = self.c.fetchall()
		self.c.close()
		return rows
		
	def selectMix(self, prev_date, ini_date, end_date, rows1, rows2):
		self.c = self.conn.cursor()
		print prev_date, " ", ini_date, " ", end_date
		self.c.execute(self.select_mix, (ini_date, end_date, prev_date, ini_date, ini_date, end_date, rows1, rows2))
		rowsA = self.c.fetchall()
		self.c.execute(self.select_lru, (ini_date, end_date, rows1))
		rowsB = self.c.fetchall()
		self.c.close()
		rowsB.extend(rowsA)
		return rowsB
		
	def selectLRU(self, ini_date, end_date, rows):
		self.c = self.conn.cursor()
		print ini_date, " ", end_date
		self.c.execute(self.select_lru, (ini_date, end_date, rows))
		rows = self.c.fetchall()
		self.c.close()
		return rows
		



