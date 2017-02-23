import argparse
import logging
from collections import defaultdict
import os
import json
import pprint
import copy



def get_ints(line):
	return map(int, line.split())

class InputFile(object):
	V = None
	E = None
	C = None
	R = None
	X = None

	VIDEOS = None

	ENDPOINTS = None

	REQUESTS = None

	SCORES = None

	VIDEOS_ENDPOINTS = None

	CACHE_VIDEOS = None

	CACHE_FREE = None

	ENDPOINTS_VIDEOS_LATENCY = None

	CANDIDATES = None

	def __init__(self, path):
		
		self.input = open(path)
		
		self.V, self.E, self.R, self.C, self.X = get_ints(self.input.readline())
		self.VIDEOS = get_ints(self.input.readline())
		self.ENDPOINTS = {}
		self.REQUESTS = defaultdict(dict)
		self.SCORES = []

		self.VIDEOS_ENDPOINTS = defaultdict(set)
		self.CACHE_VIDEOS = defaultdict(list)

		self.CACHE_FREE = {}

		self.ENDPOINTS_VIDEOS_LATENCY = defaultdict(dict)

		self.CANDIDATES = []


		
		for endpoint_id in xrange(self.E):
						
			datacenter_latency, cache_count = get_ints(self.input.readline())

			self.ENDPOINTS[endpoint_id] = {'latency': datacenter_latency, 'caches': {}}

			for i in xrange(cache_count):
				cache_id, cache_latency = get_ints(self.input.readline())
				self.ENDPOINTS[endpoint_id]['caches'][cache_id] = cache_latency

		for request_id in xrange(self.R):

			video_id, endpoint_id, requests_count = get_ints(self.input.readline())

			self.REQUESTS[video_id][endpoint_id] = requests_count

			

	def post_process(self):

		post_endpoints = {}

		for endpoint_id, endpoint in self.ENDPOINTS.iteritems():
			if endpoint['caches']:
				post_endpoints[endpoint_id] = endpoint

		self.ENDPOINTS = post_endpoints

		for video_id in self.REQUESTS:
			for endpoint_id in self.REQUESTS[video_id]:
				if endpoint_id in self.ENDPOINTS:
					self.VIDEOS_ENDPOINTS[video_id].add(endpoint_id)


		for cache_id in xrange(self.C):
			self.CACHE_FREE[cache_id] = self.X


	def parse_videos(self):

		CANDIDATES = []

		for video_id in xrange(self.V):
			for cache_id in xrange(self.C):
				score = self.compute_score(video_id, cache_id)
				self.SCORES.append((video_id, cache_id, score))

		unsorted_scores = self.SCORES

		while True:
			
			self.SCORES = sorted(self.SCORES, key=lambda x: -x[2])
			
			video_id, cache_id, score = self.SCORES[0]

			if score == 0:
				break

			self.CANDIDATES.append((video_id, cache_id))

			print 'SELECTING: ', video_id, cache_id, score

			for endpoint_id, endpoint in self.ENDPOINTS.iteritems():
				if cache_id in endpoint['caches']:

					if not video_id in self.ENDPOINTS_VIDEOS_LATENCY[endpoint_id]:
						self.ENDPOINTS_VIDEOS_LATENCY[endpoint_id][video_id] = endpoint['caches'][cache_id]
					else:
						self.ENDPOINTS_VIDEOS_LATENCY[endpoint_id][video_id] = min(self.ENDPOINTS_VIDEOS_LATENCY[endpoint_id][video_id], endpoint['caches'][cache_id])

			

			self.CACHE_FREE[cache_id] -= self.VIDEOS[video_id]

			self.CACHE_VIDEOS[cache_id].append(video_id)

			index = video_id * self.C 

			for cache_id in xrange(self.C):
				score = self.compute_score(video_id, cache_id)

				unsorted_scores[index] = (video_id, cache_id, score)
				index += 1

			self.SCORES = sorted(unsorted_scores, key=lambda x: -x[2])




	def save_output(self):

		caches = defaultdict(list)

		


		for video_id, cache_id in self.CANDIDATES:
			caches[cache_id].append(video_id)
		

		


	 	with open('output.txt', 'w') as f:

			f.write('%s\n' % len(caches))

			for cache_id in caches:
				
				f.write('%s %s\n' % (cache_id, ' '.join(map(str, sorted(caches[cache_id])))))




		

	def compute_score(self, video_id, cache_id):
		video_size = self.VIDEOS[video_id] 
		
		if video_size > self.CACHE_FREE[cache_id]:
			return 0

		score = 0

		for endpoint_id in self.VIDEOS_ENDPOINTS[video_id]:
			if cache_id in self.ENDPOINTS[endpoint_id]['caches']:



				if not video_id in self.ENDPOINTS_VIDEOS_LATENCY[endpoint_id]:
					score += (self.ENDPOINTS[endpoint_id]['latency'] - self.ENDPOINTS[endpoint_id]['caches'][cache_id]) * self.REQUESTS[video_id][endpoint_id]
				else:
					best_score = (self.ENDPOINTS[endpoint_id]['latency'] - self.ENDPOINTS_VIDEOS_LATENCY[endpoint_id][video_id]) * self.REQUESTS[video_id][endpoint_id]
					current_score = (self.ENDPOINTS[endpoint_id]['latency'] - self.ENDPOINTS[endpoint_id]['caches'][cache_id]) * self.REQUESTS[video_id][endpoint_id]
					score += max(0, current_score - best_score)

		return score




def main():

	parser = argparse.ArgumentParser(description="Google hashcode")

	parser.add_argument('-i', '--input', action='store')

	args = parser.parse_args()

	try:
		input_file = InputFile(args.input)
	except IOError:
		log.error('Error reading file')
		os.exit(1)

	input_file.post_process()
	input_file.parse_videos()
	input_file.save_output()	

	pprint.pprint(input_file.__dict__)
	

if __name__ == 	'__main__':
	main()