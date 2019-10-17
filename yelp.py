#import the modules
from __future__ import print_function 
import requests
import json
#from YelpAPI import get_my_key
# Python 2/3 compatibility
import boto3
import json
import decimal
import datetime
#define a business ID
#business_id = '         '


#define the API key, define the endpoint, and define the header
API_KEY = 'ByaaAUi_DrSi_vpG9tzD4dNq8uzQgIZU7GebHYM-03epo_pNjTvbqIlgLAspw_ohJ4VymVUxhnNAeBtBMomFyPJ0XhOLLcJf6t9PqIXXrAsRK4ZWbeeZcyKdTLCbXXYx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


y = 0

while True:
	#define the parameters
	
	if y > 950:
		break
	
	PARAMETERS = {'term':'Oriental restaurants',
			  'limit': 50,
			  'radius': 10000,
			  'offset': y,
			  'location': 'Manhattan'}

	response = requests.get(url = ENDPOINT, params = PARAMETERS, headers = HEADERS)
	parsed = json.loads(response.text.replace("\"\"","\"NA\""), parse_float=decimal.Decimal)
	y = y + 50

	#print(parsed)

	dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")

	table = dynamodb.Table('yelp-restaurants')


	for biz in parsed['businesses']:
		now = datetime.datetime.now()
		timestamp = str(now.strftime("%Y-%m-%d %H:%M"))
		biz["date"] = timestamp
		print(biz)

		resp = table.put_item(Item=biz)

		print(json.dumps(resp, indent=4, cls=DecimalEncoder))

		data = {}
		data.update({'id':biz['id'], 'cuisine':biz['categories'][0]['title']})
		data = json.dumps(data)
		print(data)

		headers = {'Content-type': 'application/json'}
		API_ENDPOINT = "https://search-cloudproject-xid6zeahpe2kwsfpx6rltxamd4.us-east-1.es.amazonaws.com/" + "restaurants/Restaurant"
		r = requests.post(url = API_ENDPOINT, data = data, headers = headers)
