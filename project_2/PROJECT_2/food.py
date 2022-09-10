from flask import Flask, request, json, Response
import requests
from collections import OrderedDict

app = Flask(__name__)

@app.route('/restaurants')
def method():
	
	try:
		add = request.args['address']
	except:
		data = {'return message': 'address parameter missing'}
		js = json.dumps(data)
		resp = Response(js, status = 400, mimetype = 'application/json')
		return resp
	
	add = add.strip()

	apiKey = 'b89da837ac38796333a633d11322d17889a3628'

	PARAMS = {'q':add, 'api_key':apiKey}

	URL = "https://api.geocod.io/v1.6/geocode"

	req = requests.get(url = URL, params = PARAMS)
	
	if req.status_code != 200:
		data = {'return message': 'Error using Service Geocodio'}
		js = json.dumps(data)
		resp = Response(js, status = 500, mimetype = 'application/json')
		return resp
	
	#loads() - Deserializes a JSON object to a standard python object.
	json_response = json.loads(req.content)

	latitude = json_response['results'][0]['location']['lat']
	longitude = json_response['results'][0]['location']['lng']

	URL = "https://api.yelp.com/v3/businesses/search"

	PARAMS = {'latitude': latitude, 'longitude': longitude, 'categories': "Food"}

	apiKey = 'ps5D7hoZEuSx__TMy412ok129wSbNs5YANjei7mHXvxCsA11oD0SS1jE4_6Bio4vSFB19Nml8gOtXA0alggMsggF96X4liGb-R0I9bAguNeTamcSNWUEm_04zJ19YXYx'

	auth_value = "Bearer " + apiKey 

	headers = {"Authorization": auth_value}

	req = requests.get(url = URL, params = PARAMS, headers=headers)

	if req.status_code != 200:
                data = {'return message': 'Error using Service Yelp'}
                js = json.dumps(data)
                resp = Response(js, status = 500, mimetype = 'application/json')
                return resp

	json_response = json.loads(req.content)

	#This is a list of dictionaries
	#each dictionary has 3 keys: Name, address, rating

	list = []

	response_restaurantList = json_response['businesses']

	for obj in response_restaurantList:
		d = OrderedDict()
		d['name'] = obj['name']

		#Format the address
		address = ""
		for element in obj['location']['display_address']:
			address += element + " "
		address = address.strip()
		
		d['address'] = address
		d['rating'] = str(obj['rating'])
		list.append(d)

	data = {}
	data["restaurants"] = list

	#dumps() - This method allows you to convert a python object into a serialized JSON object
	js = json.dumps(data)
	resp = Response(js, status = 200, mimetype = 'application/json')
	return resp


if __name__ == '__main__':
	app.run()	


