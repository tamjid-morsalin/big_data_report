import flask
from flask import request
import mysql.connector
import requests

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/tp-callback', methods=['POST'])
def tpCallback():
	requestBody = request.json
	print(requestBody)
	return "Hello"

@app.route('/callback', methods=['POST'])
def callBack():
	requestBody = request.json
	
	#establishing the connection
	conn = mysql.connector.connect(
   		user='root', 
   		password='root', 
   		host='127.0.0.1',
   		auth_plugin='mysql_native_password',
   		database='big_data_report'
	)

	cursor = conn.cursor()

	updatedId= requestBody["data"]["id"]

	if requestBody['code'] == 0:
		cursor.execute("UPDATE queries SET execution_status='SUCCESS', response_file_path='%s' WHERE id=%s" % (requestBody['data']['file'],updatedId))
	else:
		cursor.execute("UPDATE queries SET execution_status='FAILED' WHERE id=%s" % (updatedId))

	if requestBody['data']['callback']:
		try:
			response = requests.post(requestBody['data']['callback'], json = requestBody)
		except Exception as e:
			raise SystemExit(e)

	conn.commit()
	cursor.close()
	conn.close()

	return "success"
if __name__ == '__main__':
    app.run(host= '127.0.0.1',port='5000',debug=True)