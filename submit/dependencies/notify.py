import __main__
import requests
import json

def notify(data, config, request):
	callback = config['notify_path']

	response = {}

	response['status'] = 200
	response['code'] = 0
	response['title'] = "Success"
	response['details'] = "Success"
	response['data'] = {}

	response['data']['file'] = data[0]
	response['data']['id'] = request['id']

	if request['callback']:
		response['data']['callback'] = request['callback']

	#json_response_data = json.dumps(response)

	try:
		res = requests.post(callback, json = response)
	except Exception as e:
		raise SystemExit(e)