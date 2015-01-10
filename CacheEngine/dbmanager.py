#!/usr/bin/python

try:
    import json
except ImportError:
    import simplejson as json

from API import fp2
import query_log

class DbManager(object):

	fpCon = None
		
	def __init__(self, solarIP ="147.46.240.170", tyIP = "147.46.240.170", tyPort = 1978):
		self.fpCon = fp2.FpConnection("http://" + solarIP + ":8502/solr/fp", tyIP, tyPort)



	def parse_json_file(self, jfile):
		codes = json.load(open(jfile))
		fullcodes = []
		
		for c in codes:
			if "code" not in c:
				continue
			code = c["code"]
			m = c["metadata"]

			if "track_id" in m:
				trid = m["track_id"].encode("utf-8")
			else:
				trid = self.fpCon.new_track_id()
		
			length = m["duration"]
			version = m["version"]
			artist = m.get("artist", None)
			title = m.get("title", None)
			release = m.get("release", None)
			decoded = fp2.decode_code_string(code)

        
			data = {"track_id": trid,
				"fp": decoded,
				"length": length,
				"codever": "%.2f" % version
			}
			if artist: data["artist"] = artist
			if release: data["release"] = release
			if title: data["track"] = title
			fullcodes.append(data)


		return fullcodes

	def parse_data(self, trid, code, metadata):
		fullcodes = []

		m = eval(metadata)

		length = m["duration"]
		version = m["version"]
		artist = m.get("artist", None)
		title = m.get("title", None)
		release = m.get("release", None)
		decoded = fp2.decode_code_string(code)


		data = {"track_id": trid,
			"fp": decoded,
			"length": length,
			"codever": "%.2f" % version
		}
		if artist: data["artist"] = artist
		if release: data["release"] = release
		if title: data["track"] = title
		fullcodes.append(data)

		return fullcodes


	def ingestCodesFile(self, jfile):
		codes = self.parse_json_file(jfile)
		self.fpCon.ingest(codes, do_commit=False)
		self.fpCon.commit()

	def ingestCodes(self, trid, code, metadata):
		codes = self.parse_data(trid, code, metadata)
		self.fpCon.ingest(codes, do_commit=False)
		self.fpCon.commit()
		
	def ingestBySongID(self, songID, missing_msg = ""):
		q = query_log.QueryLog()
		row = q.selectRef(songID)
		if row:
			self.ingestCodes(row[0], row[1], row[2])
		else:
			print "No row ", missing_msg
	

	def clearDatabase(self):
		self.fpCon.deleteAll()



