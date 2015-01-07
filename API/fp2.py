#!/usr/bin/env python
# encoding: utf-8
"""
fp.py

Created by Brian Whitman on 2010-06-16.
Copyright (c) 2010 The Echo Nest Corporation. All rights reserved.
"""
from __future__ import with_statement
import logging
import solr
import pickle
from collections import defaultdict
import zlib, base64, re, time, random, string, math
import pytyrant
import datetime

now = datetime.datetime.utcnow()
IMPORTDATE = now.strftime("%Y-%m-%dT%H:%M:%SZ")
_hexpoch = int(time.time() * 1000)
logger = logging.getLogger(__name__)

try:
    import json
except ImportError:
    import simplejson as json


class Response(object):
    # Response codes
    NOT_ENOUGH_CODE, CANNOT_DECODE, SINGLE_BAD_MATCH, SINGLE_GOOD_MATCH, NO_RESULTS, MULTIPLE_GOOD_MATCH_HISTOGRAM_INCREASED, \
        MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED, MULTIPLE_BAD_HISTOGRAM_MATCH, MULTIPLE_GOOD_MATCH = range(9)

    def __init__(self, code, TRID=None, score=0, qtime=0, tic=0, metadata={}):
        self.code = code
        self.qtime = qtime
        self.TRID = TRID
        self.score = score
        self.total_time = int(time.time()*1000) - tic
        self.metadata = metadata

    def __len__(self):
        if self.TRID is not None:
            return 1
        else:
            return 0
        
    def message(self):
        if self.code == self.NOT_ENOUGH_CODE:
            return "query code length is too small"
        if self.code == self.CANNOT_DECODE:
            return "could not decode query code"
        if self.code == self.SINGLE_BAD_MATCH or self.code == self.NO_RESULTS or self.code == self.MULTIPLE_BAD_HISTOGRAM_MATCH:
            return "no results found (type %d)" % (self.code)
        return "OK (match type %d)" % (self.code)
    
    def match(self):
        return self.TRID is not None
     



def chunker(seq, size):
    return [tuple(seq[pos:pos + size]) for pos in xrange(0, len(seq), size)]
    
def inflate_code_string(s):
    """ Takes an uncompressed code string consisting of 0-padded fixed-width
        sorted hex and converts it to the standard code string."""
    n = int(len(s) / 10.0) # 5 hex bytes for hash, 5 hex bytes for time (40 bits)

    def pairs(l, n=2):
        """Non-overlapping [1,2,3,4] -> [(1,2), (3,4)]"""
        # return zip(*[[v for i,v in enumerate(l) if i % n == j] for j in range(n)])
        end = n
        res = []
        while end <= len(l):
            start = end - n
            res.append(tuple(l[start:end]))
            end += n
        return res

    # Parse out n groups of 5 timestamps in hex; then n groups of 8 hash codes in hex.
    end_timestamps = n*5
    times = [int(''.join(t), 16) for t in chunker(s[:end_timestamps], 5)]
    codes = [int(''.join(t), 16) for t in chunker(s[end_timestamps:], 5)]

    assert(len(times) == len(codes)) # these should match up!
    return ' '.join('%d %d' % (c, t) for c,t in zip(codes, times))

def decode_code_string(compressed_code_string):
    compressed_code_string = compressed_code_string.encode('utf8')
    if compressed_code_string == "":
        return ""
    # do the zlib/base64 stuff
    try:
        # this will decode both URL safe b64 and non-url-safe
        actual_code = zlib.decompress(base64.urlsafe_b64decode(compressed_code_string))
    except (zlib.error, TypeError):
        logger.warn("Could not decode base64 zlib string %s" % (compressed_code_string))
        import traceback; logger.warn(traceback.format_exc())
        return None
    # If it is a deflated code, expand it from hex
    if ' ' not in actual_code:
        actual_code = inflate_code_string(actual_code)
    return actual_code
    
def split_codes(fp):
    """ Split a codestring into a list of codestrings. Each string contains
        at most 60 seconds of codes, and codes overlap every 30 seconds. Given a
        track id, return track ids of the form trid-0, trid-1, trid-2, etc. """

    # Convert seconds into time units
    segmentlength = 60 * 1000.0 / 23.2
    halfsegment = segmentlength / 2.0
    
    trid = fp["track_id"]
    codestring = fp["fp"]

    codes = codestring.split()
    pairs = chunker(codes, 2)
    pairs = [(int(x[1]), " ".join(x)) for x in pairs]

    pairs.sort()
    size = len(pairs)

    if len(pairs):
        lasttime = pairs[-1][0]
        numsegs = int(lasttime / halfsegment) + 1
    else:
        numsegs = 0

    ret = []
    sindex = 0
    for i in range(numsegs):
        s = i * halfsegment
        e = i * halfsegment + segmentlength
        #print i, s, e
        
        while sindex < size and pairs[sindex][0] < s:
            #print "s", sindex, l[sindex]
            sindex+=1
        eindex = sindex
        while eindex < size and pairs[eindex][0] < e:
            #print "e",eindex,l[eindex]
            eindex+=1
        key = "%s-%d" % (trid, i)
        
        segment = {"track_id": key,
                   "fp": " ".join((p[1]) for p in pairs[sindex:eindex]),
                   "length": fp["length"],
                   "codever": fp["codever"]}
        if "artist" in fp: segment["artist"] = fp["artist"]
        if "release" in fp: segment["release"] = fp["release"]
        if "track" in fp: segment["track"] = fp["track"]
        if "source" in fp: segment["source"] = fp["source"]
        if "import_date" in fp: segment["import_date"] = fp["import_date"]
        ret.append(segment)
    return ret   
    
def cut_code_string_length(code_string):
    """ Remove all codes from a codestring that are > 60 seconds in length.
    Because we can only match 60 sec, everything else is unnecessary """
    split = code_string.split()
    if len(split) < 2:
        return code_string

    # If we use the codegen on a file with start/stop times, the first timestamp
    # is ~= the start time given. There might be a (slightly) earlier timestamp
    # in another band, but this is good enough
    first_timestamp = int(split[1])
    sixty_seconds = int(60.0 * 1000.0 / 23.2 + first_timestamp)
    parts = []
    for (code, t) in zip(split[::2], split[1::2]):
        tstamp = int(t)
        if tstamp <= sixty_seconds:
            parts.append(code)
            parts.append(t)
    return " ".join(parts)

class FpConnection(object):
	_fp_solr = solr.SolrConnectionPool("http://147.46.240.168:8502/solr/fp")
	_tyrant_address = ['147.46.240.168', 1978]
	_tyrant = None
	
		
	def __init__(self, connection, tyIP, tyPort):
		self._fp_solr = solr.SolrConnectionPool(connection)
		self._tyrant_address = [tyIP, tyPort]
		self._tyrant = pytyrant.PyTyrant.open(*self._tyrant_address)


	def metadata_for_track_id(self, track_id, local=False):
		if not track_id or not len(track_id):
		    return {}
		# Assume track_ids have 1 - and it's at the end of the id.
		if "-" not in track_id:
		    track_id = "%s-0" % track_id
		    		    
		with solr.pooled_connection(self._fp_solr) as host:
		    response = host.query("track_id:%s" % track_id)

		if len(response.results):
		    return response.results[0]
		else:
		    return {}

	def best_match_for_query(self, code_string, elbow=10, local=False):
		# DEC strings come in as unicode so we have to force them to ASCII
		orig_code_string = code_string
		code_string = code_string.encode("utf8")
		tic = int(time.time()*1000)

		# First see if this is a compressed code
		if re.match('[A-Za-z\/\+\_\-]', code_string) is not None:
		    code_string = decode_code_string(code_string)
		    if code_string is None:
		        return Response(Response.CANNOT_DECODE, tic=tic)
		
		code_len = len(code_string.split(" ")) / 2
		if code_len < elbow:
		    logger.warn("Query code length (%d) is less than elbow (%d) (%s)" % (code_len, elbow, orig_code_string))
		    return Response(Response.NOT_ENOUGH_CODE, tic=tic)

		code_string = cut_code_string_length(code_string)
		code_len = len(code_string.split(" ")) / 2

		# Query the FP flat directly.
		response = self.query_fp(code_string, rows=30, local=local, get_data=True)
		logger.debug("solr qtime is %d" % (response.header["QTime"]))
		
		if len(response.results) == 0:
		    return Response(Response.NO_RESULTS, qtime=response.header["QTime"], tic=tic)

		# If we just had one result, make sure that it is close enough. We rarely if ever have a single match so this is not helpful (and probably doesn't work well.)
		top_match_score = int(response.results[0]["score"])
		if len(response.results) == 1:
		    trackid = response.results[0]["track_id"]
		    trackid = trackid.split("-")[0] # will work even if no `-` in trid
		    meta = self.metadata_for_track_id(trackid, local=local)
		    if code_len - top_match_score < elbow:
		        return Response(Response.SINGLE_GOOD_MATCH, TRID=trackid, score=top_match_score, qtime=response.header["QTime"], tic=tic, metadata=meta)
		    else:
		        return Response(Response.SINGLE_BAD_MATCH, qtime=response.header["QTime"], tic=tic)

		# If the scores are really low (less than 5% of the query length) then say no results
		if top_match_score < code_len * 0.05:
		    return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime = response.header["QTime"], tic=tic)

		# Not a strong match, so we look up the codes in the keystore and compute actual matches...

		# Get the actual score for all responses
		original_scores = {}
		actual_scores = {}
		
		trackids = [r["track_id"].encode("utf8") for r in response.results]
		if local:
		    tcodes = [_fake_solr["store"][t] for t in trackids]
		else:
		    tcodes = self.get_tyrant().multi_get(trackids)
		
		# For each result compute the "actual score" (based on the histogram matching)
		for (i, r) in enumerate(response.results):
		    track_id = r["track_id"]
		    original_scores[track_id] = int(r["score"])
		    track_code = tcodes[i]
		    if track_code is None:
		        # Solr gave us back a track id but that track
		        # is not in our keystore
		        continue
		    actual_scores[track_id] = self.actual_matches(code_string, track_code, elbow = elbow)
		
		#logger.debug("Actual score for %s is %d (code_len %d), original was %d" % (r["track_id"], actual_scores[r["track_id"]], code_len, top_match_score))
		# Sort the actual scores
		sorted_actual_scores = sorted(actual_scores.iteritems(), key=lambda (k,v): (v,k), reverse=True)
		
		# Because we split songs up into multiple parts, sometimes the results will have the same track in the
		# first few results. Remove these duplicates so that the falloff is (potentially) higher.
		new_sorted_actual_scores = []
		existing_trids = []
		for trid, result in sorted_actual_scores:
		    trid_split = trid.split("-")[0]
		    if trid_split not in existing_trids:
		        new_sorted_actual_scores.append((trid, result))
		        existing_trids.append(trid_split)
		sorted_actual_scores = new_sorted_actual_scores

		# We might have reduced the length of the list to 1
		if len(sorted_actual_scores) == 1:
		    logger.info("only have 1 score result...")
		    (top_track_id, top_score) = sorted_actual_scores[0]
		    if top_score < code_len * 0.1:
		        logger.info("only result less than 10%% of the query string (%d < %d *0.1 (%d)) SINGLE_BAD_MATCH", top_score, code_len, code_len*0.1)
		        return Response(Response.SINGLE_BAD_MATCH, qtime = response.header["QTime"], tic=tic)
		    else:
		        if top_score > (original_scores[top_track_id] / 2): 
		            logger.info("top_score > original_scores[%s]/2 (%d > %d) GOOD_MATCH_DECREASED",
		                top_track_id, top_score, original_scores[top_track_id]/2)
		            trid = top_track_id.split("-")[0]
		            meta = self.metadata_for_track_id(trid, local=local)
		            return Response(Response.MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED, TRID=trid, score=top_score, qtime=response.header["QTime"], tic=tic, metadata=meta)
		        else:
		            logger.info("top_score NOT > original_scores[%s]/2 (%d <= %d) BAD_HISTOGRAM_MATCH",
		                top_track_id, top_score, original_scores[top_track_id]/2)
		            return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime=response.header["QTime"], tic=tic)
		    
		# Get the top one
		(actual_score_top_track_id, actual_score_top_score) = sorted_actual_scores[0]
		# Get the 2nd top one (we know there is always at least 2 matches)
		(actual_score_2nd_track_id, actual_score_2nd_score) = sorted_actual_scores[1]

		trackid = actual_score_top_track_id.split("-")[0]
		meta = self.metadata_for_track_id(trackid, local=local)
		
		if actual_score_top_score < code_len * 0.05:
		    return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime = response.header["QTime"], tic=tic)
		else:
		    # If the actual score went down it still could be close enough, so check for that
		    if actual_score_top_score > (original_scores[actual_score_top_track_id] / 4): 
		        if (actual_score_top_score - actual_score_2nd_score) >= (actual_score_top_score / 3):  # for examples [10,4], 10-4 = 6, which >= 5, so OK
		            return Response(Response.MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED, TRID=trackid, score=actual_score_top_score, qtime=response.header["QTime"], tic=tic, metadata=meta)
		        else:
		            return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime = response.header["QTime"], tic=tic)
		    else:
		        # If the actual score was not close enough, then no match.
		        return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime=response.header["QTime"], tic=tic)

	def actual_matches(self, code_string_query, code_string_match, slop = 2, elbow = 10):
		code_query = code_string_query.split(" ")
		code_match = code_string_match.split(" ")
		if (len(code_match) < (elbow*2)):
		    return 0

		time_diffs = {}

		# Normalise the query timecodes to start with offset 0
		code_query_int = [int(x) for x in code_query]
		min_time = min(code_query_int[1::2])
		code_query[1::2] = [str(x - min_time) for x in code_query_int[1::2]]
		
		#
		# Invert the query codes
		query_codes = {}
		for (qcode, qtime) in zip(code_query[::2], code_query[1::2]):
		    qtime = int(qtime) / slop
		    if qcode in query_codes:
		        query_codes[qcode].append(qtime)
		    else:
		        query_codes[qcode] = [qtime]

		#
		# Walk the document codes, handling those that occur in the query
		match_counter = 1
		for match_code in code_match[::2]:
		    if match_code in query_codes:
		        match_code_time = int(code_match[match_counter])/slop
		        min_dist = 32767
		        for qtime in query_codes[match_code]:
		            # match_code_time > qtime for all corresponding
		            # hashcodes since normalising query timecodes, so no
		            # need for abs() anymore
		            dist = match_code_time - qtime
		            if dist < min_dist:
		                min_dist = dist
		        if min_dist < 32767:
		            if time_diffs.has_key(min_dist):
		                time_diffs[min_dist] += 1
		            else:
		                time_diffs[min_dist] = 1
		    match_counter += 2

		# sort the histogram, pick the top 2 and return that as your actual score
		actual_match_list = sorted(time_diffs.iteritems(), key=lambda (k,v): (v,k), reverse=True)

		if(len(actual_match_list)>1):
		    return actual_match_list[0][1] + actual_match_list[1][1]
		if(len(actual_match_list)>0):
		    return actual_match_list[0][1]
		return 0   
		
		
	def best_match_for_query_cache(self, code_string, elbow=100, local=False):
		# DEC strings come in as unicode so we have to force them to ASCII
		orig_code_string = code_string
		code_string = code_string.encode("utf8")
		tic = int(time.time()*1000)
		percentage_len = 0.10
		percentage_original = 0.50
		perc_dif_scond = 0.50

		# First see if this is a compressed code
		if re.match('[A-Za-z\/\+\_\-]', code_string) is not None:
		    code_string = decode_code_string(code_string)
		    if code_string is None:
		        return Response(Response.CANNOT_DECODE, tic=tic)
		
		code_len = len(code_string.split(" ")) / 2
		if code_len < elbow:
		    logger.warn("Query code length (%d) is less than elbow (%d)  (%s)" % (code_len, elbow, orig_code_string))
		    return Response(Response.NOT_ENOUGH_CODE, tic=tic)

		code_string = cut_code_string_length(code_string)
		code_len = len(code_string.split(" ")) / 2

		# Query the FP flat directly.
		response = self.query_fp(code_string, rows=30, local=local, get_data=True)
		logger.debug("solr qtime is %d" % (response.header["QTime"]))
		
		if len(response.results) == 0:
		    return Response(Response.NO_RESULTS, qtime=response.header["QTime"], tic=tic)

		# If we just had one result, make sure that it is close enough. We rarely if ever have a single match so this is not helpful (and probably doesn't work well.)
		top_match_score = int(response.results[0]["score"])
		if len(response.results) == 1:
		    trackid = response.results[0]["track_id"]
		    trackid = trackid.split("-")[0] # will work even if no `-` in trid
		    meta = self.metadata_for_track_id(trackid, local=local)
		    if code_len - top_match_score < elbow:
		        return Response(Response.SINGLE_GOOD_MATCH, TRID=trackid, score=top_match_score, qtime=response.header["QTime"], tic=tic, metadata=meta)
		    else:
		        return Response(Response.SINGLE_BAD_MATCH, qtime=response.header["QTime"], tic=tic)

		# If the scores are really low (less than 5% (percentage_len) of the query length) then say no results
		if top_match_score < code_len * percentage_len:
		    return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime = response.header["QTime"], tic=tic)

		# Not a strong match, so we look up the codes in the keystore and compute actual matches...

		# Get the actual score for all responses
		original_scores = {}
		actual_scores = {}
		
		trackids = [r["track_id"].encode("utf8") for r in response.results]
		if local:
		    tcodes = [_fake_solr["store"][t] for t in trackids]
		else:
		    tcodes = self.get_tyrant().multi_get(trackids)
		
		# For each result compute the "actual score" (based on the histogram matching)
		for (i, r) in enumerate(response.results):
		    track_id = r["track_id"]
		    original_scores[track_id] = int(r["score"])
		    track_code = tcodes[i]
		    if track_code is None:
		        # Solr gave us back a track id but that track
		        # is not in our keystore
		        continue
		    actual_scores[track_id] = self.actual_matches(code_string, track_code, elbow = elbow)
		
		#logger.debug("Actual score for %s is %d (code_len %d), original was %d" % (r["track_id"], actual_scores[r["track_id"]], code_len, top_match_score))
		# Sort the actual scores
		sorted_actual_scores = sorted(actual_scores.iteritems(), key=lambda (k,v): (v,k), reverse=True)
		
		# Because we split songs up into multiple parts, sometimes the results will have the same track in the
		# first few results. Remove these duplicates so that the falloff is (potentially) higher.
		new_sorted_actual_scores = []
		existing_trids = []
		for trid, result in sorted_actual_scores:
		    trid_split = trid.split("-")[0]
		    if trid_split not in existing_trids:
		        new_sorted_actual_scores.append((trid, result))
		        existing_trids.append(trid_split)
		sorted_actual_scores = new_sorted_actual_scores

		# We might have reduced the length of the list to 1
		if len(sorted_actual_scores) == 1:
		    logger.info("only have 1 score result...")
		    (top_track_id, top_score) = sorted_actual_scores[0]
		    if top_score < code_len * 0.1:
		        logger.info("only result less than 10%% of the query string (%d < %d *0.1 (%d)) SINGLE_BAD_MATCH", top_score, code_len, code_len*0.1)
		        return Response(Response.SINGLE_BAD_MATCH, qtime = response.header["QTime"], tic=tic)
		    else:
		        if top_score > (original_scores[top_track_id] / 2): 
		            logger.info("top_score > original_scores[%s]/2 (%d > %d) GOOD_MATCH_DECREASED",
		                top_track_id, top_score, original_scores[top_track_id]/2)
		            trid = top_track_id.split("-")[0]
		            meta = self.metadata_for_track_id(trid, local=local)
		            return Response(Response.MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED, TRID=trid, score=top_score, qtime=response.header["QTime"], tic=tic, metadata=meta)
		        else:
		            logger.info("top_score NOT > original_scores[%s]/2 (%d <= %d) BAD_HISTOGRAM_MATCH",
		                top_track_id, top_score, original_scores[top_track_id]/2)
		            return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime=response.header["QTime"], tic=tic)
		    
		# Get the top one
		(actual_score_top_track_id, actual_score_top_score) = sorted_actual_scores[0]
		# Get the 2nd top one (we know there is always at least 2 matches)
		(actual_score_2nd_track_id, actual_score_2nd_score) = sorted_actual_scores[1]

		trackid = actual_score_top_track_id.split("-")[0]
		meta = self.metadata_for_track_id(trackid, local=local)
		
		#PBM min percentage_len for cache
		if actual_score_top_score < code_len * percentage_len:
		    return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime = response.header["QTime"], tic=tic)
		else:
		    # If the actual score went down it still could be close enough, so check for that
			#PBM percentage_original
		    if actual_score_top_score > (original_scores[actual_score_top_track_id] * percentage_original): 
				#PBM perc_dif_scond percetange diff second best score
		        if (actual_score_top_score - actual_score_2nd_score) >= (actual_score_top_score * perc_dif_scond):  # for examples [10,4], 10-4 = 6, which >= 5, so OK
		            return Response(Response.MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED, TRID=trackid, score=actual_score_top_score, qtime=response.header["QTime"], tic=tic, metadata=meta)
		        else:
		            return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime = response.header["QTime"], tic=tic)
		    else:
		        # If the actual score was not close enough, then no match.
		        return Response(Response.MULTIPLE_BAD_HISTOGRAM_MATCH, qtime=response.header["QTime"], tic=tic)
		
	def query_fp(self, code_string, rows=15, local=False, get_data=False):
	
		try:
		    # query the fp flat
		    if get_data:
		        fields = "track_id,artist,release,track,length"
		    else:
		        fields = "track_id"
		    with solr.pooled_connection(self._fp_solr) as host:
		        resp = host.query(code_string, qt="/hashq", rows=rows, fields=fields)
		    return resp
		except solr.SolrException:
		    return None     

	def get_tyrant(self):
		return self._tyrant
		
		
	def ingest(self, fingerprint_list, do_commit=True, local=False, split=True):
		""" Ingest some fingerprints into the fingerprint database.
		    The fingerprints should be of the form
		      {"track_id": id,
		      "fp": fp string,
		      "artist": artist,
		      "release": release,
		      "track": track,
		      "length": length,
		      "codever": "codever",
		      "source": source,
		      "import_date":import date}
		    or a list of the same. All parameters except length must be strings. Length is an integer.
		    artist, release and track are not required but highly recommended.
		    The import date should be formatted as an ISO 8601 date (yyyy-mm-ddThh:mm:ssZ) and should
		    be the UTC time that the the import was performed. If the date is missing, the time the
		    script was started will be used.
		    length is the length of the track being ingested in seconds.
		    if track_id is empty, one will be generated.
		"""
		if not isinstance(fingerprint_list, list):
		    fingerprint_list = [fingerprint_list]
		    
		docs = []
		codes = []
		if split:
		    for fprint in fingerprint_list:
		        if not ("track_id" in fprint and "fp" in fprint and "length" in fprint and "codever" in fprint):
		            raise Exception("Missing required fingerprint parameters (track_id, fp, length, codever")
		        if "import_date" not in fprint:
		            fprint["import_date"] = IMPORTDATE
		        if "source" not in fprint:
		            fprint["source"] = "local"
		        
		        try:
		        	split_prints = split_codes(fprint)
		        except:
		        	print fprint
		        	continue
		        	
		        docs.extend(split_prints)
		        codes.extend(((c["track_id"].encode("utf-8"), c["fp"].encode("utf-8")) for c in split_prints))
		else:
		    docs.extend(fingerprint_list)
		    codes.extend(((c["track_id"].encode("utf-8"), c["fp"].encode("utf-8")) for c in fingerprint_list))


		with solr.pooled_connection(self._fp_solr) as host:
		    host.add_many(docs)

		self.get_tyrant().multi_set(codes)

		if do_commit:
		    commit()

	def commit(self, local=False):
		with solr.pooled_connection(self._fp_solr) as host:
		    host.commit()
		    
	def deleteAll(self):
		with solr.pooled_connection(self._fp_solr) as host:
				host.delete_query("*:*") 
				host.commit()

		self.get_tyrant().clear()
		


	def new_track_id(self):
		rand5 = ''.join(random.choice(string.letters) for x in xrange(5)).upper()
		global _hexpoch
		_hexpoch += 1
		hexpoch = str(hex(_hexpoch))[2:].upper()
		## On 32-bit machines, the number of milliseconds since 1970 is 
		## a longint. On 64-bit it is not.
		hexpoch = hexpoch.rstrip('L')
		return "TR" + rand5 + hexpoch
		

