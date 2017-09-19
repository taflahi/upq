from datetime import datetime
from dateutil import parser
import json
import uuid
import pytz
import MySQLdb

esURL = 'localhost:9200/UR'
mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = '123456'
mysqlPort = 3306

def test():
	query = {
		'action' : 'buy',
		'user' : '3',
		'businessId' : 'Dx',
		'item' : '4'
	}

	return insert(completeQuery(simpleQuery(query)))

def simple(query):
	return insert(completeQuery(simpleQuery(query)))

def complete(query):
	return insert(completeQuery(query))		

def simpleQuery(query):
	query['event'] = query['action']
	query['entityType'] = 'user'
	query['entityId'] = query['business_id'] + ';' + str(query['user'])
	query['targetEntityType'] = 'item'
	query['targetEntityId'] = query['business_id'] + ';' + str(query['item'])
	

	return query

def completeQuery(query):
	if 'properties' not in query:
		query['properties'] = json.dumps({})
	else:
		query['properties'] = json.dumps(query['properties'])

	if 'targetEntityId' not in query:
		query['targetEntityId'] = None

	if 'targetEntityType' not in query:
		query['targetEntityType'] = None

	if 'eventTime' not in query:
		query['eventTime'] = datetime.utcnow().replace(tzinfo = pytz.utc, microsecond = 0)
	else:
		query['eventTime'] = parser.parse(query['eventTime'])

	return query

def insert(query):
	eventId = str(uuid.uuid4()).replace('-', '')

	db = dbConnect('pio')
	cursor = db.cursor()
	
	sql = "insert into pio_event_1 (id, event, entityType, entityId, targetEntityId, targetEntityType, properties, eventTime, eventTimeZone, creationTimeZone) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
		% (eventId, query['event'], query['entityType'], query['entityId'], query['targetEntityId'], query['targetEntityType'], query['properties'], query['eventTime'].strftime('%Y-%m-%d %H:%M:%S'), 'UTC', 'UTC')

	if query['targetEntityId'] == None:
		sql = sql = "insert into pio_event_1 (id, event, entityType, entityId, properties, eventTime, eventTimeZone, creationTimeZone) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
		% (eventId, query['event'], query['entityType'], query['entityId'], query['properties'], query['eventTime'].strftime('%Y-%m-%d %H:%M:%S'), 'UTC', 'UTC')
	print sql

	ack = None
	try:
	   cursor.execute(sql)
	   db.commit()
	   ack = 'true'
	except:
	   db.rollback()
	   ack = 'false'
	db.close()

	return {
		'ack' : ack
	}

def dbConnect(dbName):
	db = MySQLdb.connect(host=mysqlHost, port=mysqlPort,\
		user=mysqlUser, passwd=mysqlPassword, db=dbName)
	return db