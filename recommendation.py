import unirest
import json
import MySQLdb

esURL = 'localhost:9200/UR'
mysqlHost = 'localhost'
mysqlUser = 'root'
mysqlPassword = '123456'
mysqlPort = 3306
size = 5

def predict(query):
	queryBuilt = buildQuery(query)
	response = esQuery(queryBuilt)

	log(query['businessId'])

	return buildResponse(response)

def buildResponse(response):
	responseItem = {
		'itemScores' : response['hits']['hits']
	}
	
	itemScores = []
	for item in responseItem['itemScores']:
		singleItem = {}

		for detail in item['_source']['detail']:
			itemDetail = detail.split(':', 1)
			singleItem[itemDetail[0]] = itemDetail[1]

		itemScores.append(singleItem)

	return itemScores

def buildQuery(rawQuery):
	query = rawQuery
	query['user'] = rawQuery['businessId'] + ';' + rawQuery['user']
	query['item'] = rawQuery['businessId'] + ';' + rawQuery['item']
	if 'fields' not in query:
		query['fields'] = []

	query['fields'].append(buildBusinessField(rawQuery['businessId']))

	boostableEvents = getBiasedRecentUserActions(query)
	boostable = boostableEvents[0]
	events = boostableEvents[1]

	shouldQuery = buildQueryShould(query, boostable)
	mustQuery = buildQueryMust(query, boostable)
	mustNotQuery = buildQueryMustNot(query, events)
	sortQuery = buildQuerySort()

	query = {
		"size" : size,
		"query" : {
			"bool" : {
				"should" : shouldQuery,
				"must" : mustQuery,
				"must_not" : mustNotQuery,
				"minimum_should_match" : 1
			}
		},
		"sort" : sortQuery
	}

	return query

def buildQueryShould(query, boostable):
	recentUserHistory = boostable
	similarItem = getBiasedSimilarItem(query)
	boostedMetaData = getBoostedMetadata(query)

	allCorellator = recentUserHistory + similarItem + boostedMetaData

	shouldQuery = []
	for corellator in allCorellator:
		terms = {
			'terms' : {
				corellator[0] : corellator[1]
			}
		}

		shouldQuery.append(terms)

	constantScore = {
		'constant_score' : {
			'filter' : {
				'match_all' : {}
			},
			'boost' : 0
		}
	}

	shouldQuery.append(constantScore)

	return shouldQuery

def buildQueryMust(query, boostable):
	filteringMetaData = getFilteringMetadata(query)
	allCorellator = filteringMetaData

	mustQuery = []
	for corellator in allCorellator:
		terms = {
			'terms' : {
				corellator[0] : corellator[1],
				'boost' : 0
			}
		}

		mustQuery.append(terms)

	return mustQuery

def buildQueryMustNot(query, events):
	mustNotItem = []
	for event in events:
		if event['event'] == 'buy' \
		and event['targetEntityId'] != None \
		and event['targetEntityId'] not in mustNotItem:
			mustNotItem.append(event['targetEntityId'])

	if 'item' in query:
		mustNotItem.append(query['item'])

	mustNotQuery = []
	terms = {
		'ids' : {
			'values' : mustNotItem,
			'boost' : 0
		}
	}

	mustNotQuery.append(terms)

	return mustNotQuery

def buildQuerySort():
	sort = [
		{
			'_score' : {
				'order' : 'desc'
			}
		}, {
			'popRank' : {
				'unmapped_type' : 'double',
				'order' : 'desc'
			}
		}
	]

	return sort

def getBiasedRecentUserActions(query):
	response = [
		['buy', [], 0],
		['view', [], 0]
	]

	events = []

	if 'user' in query:
		userId = query['user']

		db = dbConnect('pio')
		cursor = db.cursor()
		sql = "select event, targetEntityId \
				from pio_event_1 where entityId='%s' \
				and entityType='user' \
				and event in ('buy', 'view') \
				order by eventTime desc;" \
			% (userId)

		buyEvents = []
		viewEvents = []
		try:
			cursor.execute(sql)
		   	results = cursor.fetchall()
			for row in results:
				if row[0] == 'buy' and row[1] not in buyEvents:
					buyEvents.append(row[1])
				elif row[0] == 'view' and row[1] not in viewEvents:
					viewEvents.append(row[1])

		except:
		   print "Error: unable to fetch data"
		db.close()

		response = [
			['buy', buyEvents[:2000], 0],
			['view', buyEvents[:2000], 0],
		]

	return [response, events]

def getBiasedSimilarItem(query):
	response = [
		['buy', [], 0],
		['view', [], 0]
	]

	if 'item' in query:
		itemId = query['item']
		item = getItem(itemId)
		buyEvents = item['_source']['buy']
		viewEvents = item['_source']['view']

		response = [
			['buy', buyEvents[:2000], 0],
			['view', buyEvents[:2000], 0],
		]

	return response

def getBoostedMetadata(query):
	response = []
	if 'fields' in query:
		fields = query['fields']
		for field in fields:
			if field['bias'] > 0.0:
				singleField = [field['name'], field['value'], \
					field['bias']]
				response.append(singleField)

	return response

def getFilteringMetadata(query):
	response = []
	if 'fields' in query:
		fields = query['fields']
		for field in fields:
			if field['bias'] < 0:
				singleField = [field['name'], field['value'], \
					field['bias']]
				response.append(singleField)

	return response

def buildBusinessField(businessId):
	return {
		'name' : 'businessId',
		'value' : [businessId],
		'bias' : -1
	}

def getItem(itemId):
	response = unirest.get(url = esURL + 'items/' + itemId)
	return response.body

def esQuery(query):
	response = unirest.post(url = esURL + '_search', \
		params = json.dumps(query))

	return response.body

def dbConnect(dbName):
	db = MySQLdb.connect(host=mysqlHost, port=mysqlPort,\
		user=mysqlUser, passwd=mysqlPassword, db=dbName)
	return db

def log(businessId):
	db = dbConnect('ren')
	cursor = db.cursor()
	sql = "insert into request_log (businessId) values ('%s')" \
		% (businessId)

	try:
	   cursor.execute(sql)
	   db.commit()
	except:
	   db.rollback()
	db.close()
