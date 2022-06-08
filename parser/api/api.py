import flask
from flask import request
import json
import mysql.connector
from pathlib import Path

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.parse import search
import requests

app = flask.Flask(__name__)
app.config["DEBUG"] = True

def parse_table(query):
	table_name = search('FROM {:w} ', query)
	return table_name[0]

@app.route('/process', methods=['POST'])
def process():
	requestBody = request.json

	query = requestBody['query']
	query_type = requestBody['type']
	callback = requestBody['callback']
	connection = requestBody['connection']

	response = {}
	response['query'] = query
	response['type'] = query_type
	response['callback'] = callback
	response['connection'] = connection

	table_name = parse_table(query)
	response['structure'] = {}
	response['structure']['FROM'] = {}
	response['structure']['FROM']['keyspace'] = connection['databse']
	response['structure']['FROM']['table'] = table_name

	#establishing the connection
	conn = mysql.connector.connect(
   		user='root', 
   		password='root', 
   		host='127.0.0.1',
   		auth_plugin='mysql_native_password',
   		database='big_data_report'
	)

	#Creating a cursor object using the cursor() method
	cursor = conn.cursor()

	#fetch last row from query table
	cursor.execute("SELECT * FROM queries ORDER BY id DESC LIMIT 1")
	fetch_info = cursor.fetchone()
	if fetch_info is None:
		id=1
	else:
		id=fetch_info[0]+1
	
	directory = Path().absolute()
	query_json_file_path = Path(directory).parents[0]
	file_path = str(query_json_file_path)+"/request/"+str(id)+"_request.json"
	response['id'] = id
	with open(file_path, "w") as outfile:
		json.dump(response,outfile)

	insert_query_for_query_table = ("INSERT INTO queries "
               "(request_file_path, execution_status, callback) "
               "VALUES (%s, %s, %s)")

	data_for_query_table = (file_path, 'INITIATED', callback)

	# Insert new query info
	cursor.execute(insert_query_for_query_table, data_for_query_table)
	query_no = cursor.lastrowid
	conn.commit()
	cursor.close()
	conn.close()
	return "success"

if __name__ == '__main__':
    app.run(host= '127.0.0.1',port='3000',debug=True)