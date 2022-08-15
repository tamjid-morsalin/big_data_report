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

	#establishing the connection
	conn = getDBConnection()

	#Creating a cursor object using the cursor() method
	cursor = conn.cursor()
	try:
		requestBody = request.json
		response = {}
		query = requestBody['query']
		query_type = requestBody['type']
		callback = requestBody['callback']
		connection = requestBody['connection']

		response['query'] = query
		response['type'] = query_type
		response['callback'] = callback
		response['connection'] = connection
		response['structure'] = {}
		response['structure']['FROM'] = {}
		response['structure']['FROM']['keyspace'] = connection['database']
		response['structure']['FROM']['table'] = parse_table(query)

		#fetch last row from query table
		cursor.execute("SELECT * FROM queries ORDER BY id DESC LIMIT 1")
		fetch_info = cursor.fetchone()
		if fetch_info is None:
			id=1
		else:
			id=fetch_info[0]+1
		response['id'] = id

		directory = Path().absolute()
		query_json_file_path = Path(directory).parents[0]
		filePath = str(query_json_file_path)+"/request/"+str(id)+"_request.json"
		with open(filePath, "w") as outfile:
			json.dump(response,outfile)

	except KeyError as ke:
		filePath = None
		executionStatus = 'PARSER_FAILED'
	except FileNotFoundError as fnf:
		filePath = None
		executionStatus = 'PARSER_FAILED'
	except Exception as e:
		filePath = None
		executionStatus = 'PARSER_FAILED'
	else:
		executionStatus = 'INITIATED'
	finally:
		insert_query_for_query_table = ("INSERT INTO queries "
               "(request_file_path, execution_status) "
               "VALUES (%s, %s)")
		data_for_query_table = (filePath, executionStatus)
		# Insert new query info
		cursor.execute(insert_query_for_query_table, data_for_query_table)

		query_no = cursor.lastrowid
		conn.commit()
		cursor.close()
		conn.close()
		return "success"

if __name__ == '__main__':
    app.run(host= '127.0.0.1',port='3000',debug=True)