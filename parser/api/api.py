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

def getDBConnection():
	db_config_file = open(os.getcwd()+"/configs/config.database.json")
	db_config_data = json.load(db_config_file)
	db_config_file.close()

	config = db_config_data['develop']

	return mysql.connector.connect(
   		user=config['user'], 
   		password=config['password'], 
   		host=config['host'],
   		auth_plugin=config['auth'],
   		database=config['database']
	)

def parse_table(query):
	result_table = []
	table_name = search(' FROM {:w} ', query)
	join_table_name = search(' JOIN {:w} ', query)

	if table_name is not None:
		for x in table_name:
			result_table.append(x)
	if join_table_name is not None:
		for x in join_table_name:
			result_table.append(x)

	return result_table

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
	response['structure']['FROM']['keyspace'] = connection['database']
	response['structure']['FROM']['table'] = table_name

	#establishing the connection
	conn = getDBConnection()

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