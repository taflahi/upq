from flask import Flask
from flask import abort
from flask import request
from flask import jsonify
import recommendation
import event

app = Flask(__name__)

# HTTP SAMPLES

@app.route("/recommend", methods=['GET'])
def recommend():
	if not request.args.get('user') or \
		not request.args.get('item') or \
		not request.args.get('business_id'):
		abort(400)

	query = {
        "user" : request.args.get('user'),
        "item" : request.args.get('item'),
        "businessId" : request.args.get('business_id')
    }
	response = recommendation.predict(query)
	return jsonify(response)

@app.route("/sevent", methods=['POST'])
def simpleEvent():
	print request.get_json(force = True)
	query = request.get_json(force = True)
	if 'action' not in query or \
		'user' not in query or \
		'item' not in query or \
		'business_id' not in query:
		abort(400)

	response = event.simple(query)
	return jsonify(response)

@app.route("/cevent", methods=['POST'])
def completeEvent():
	query = request.get_json(force = True)
	if 'event' not in query or \
		'entityId' not in query or \
		'entityType' not in query:
		abort(400)

	response = event.complete(query)
	return jsonify(response)

if __name__ == '__main__':
	app.run()