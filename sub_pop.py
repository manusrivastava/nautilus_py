from nautilus.nautilusdive import NautilusDive
import pymongo
import json
import requests
from pymongo import MongoClient
def get_segmentId(segment_name):
	url = 'http://10.0.2.68:8888/index/findorcreate'
	data = {"name": segment_name }
	headers = {'Content-Type': 'application/json'}
	r = requests.post(url, data=json.dumps(data), headers=headers)
	return r.json()["response"]["segment"]["segmentID"]

client = MongoClient('10.0.2.244', 27017)
db = client.subscription_qa
config = {}
config['brokers'] = 'ip-10-0-2-244:9092'
config['topic'] = 'test_subscribe'
config['partitions'] =8 
config['partitioner'] = "RoundRobinPartitioner"
k = 'pcs'
p = NautilusDive(config)
num = 0
segments = {}
segments["1:1"] = get_segmentId("1:1")
segments["1:4"] = get_segmentId("1:4")

arr = []
for data in db.channel_subscriptions.find({'tag_id': {'$in':[1,4]}, 'tag_type':1, 'status':1}).batch_size(1000):
	uid = data["user_id"]
	tagId = data["tag_id"]
	tagType =  data["tag_type"]
	key = str(tagType) + ":" + str(tagId)
	if (key in segments):
		# segId =  get_segmentId(key)
		sub = True
		obj = dict(uid=uid,segId=segments[key],sub=sub)
		msg = json.dumps(obj)
		#print msg
		#msg = msg.encode('utf-16be')
		#arr.append(uid)
		p.send(k,msg)
	num += 1
	if (num % 1000) == 0:
		print "Done: %d" % num
#print json.dumps(arr)



