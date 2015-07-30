import mysql.connector
import time
from nautilus.nautilusdive import NautilusDive

cnx = mysql.connector.connect(user='platform',database='userdb',password='p1@tf0rmD1st',host='10.0.2.244', port=3307)
cursor = cnx.cursor()

config = {}
config['brokers'] = 'ip-10-0-2-244:9092'
config['topic'] = 'user_events'
config['partitions'] = 20
k = 'puc'
p = NautilusDive(config)
q="select id, hike_uid from platform_user where status='active'"
cursor.execute(q)

for id, hike_uid in cursor:
	ts = int(time.time())
	msg = '{"uid":"%s", "pid":%d , "ts":%d}' %(hike_uid, id,ts)
	print msg
	p.send(k, msg)

cursor.close()
cnx.close()
