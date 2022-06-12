from datetime import datetime
import mysql.connector
import json
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pathlib import Path
import requests
import threading
import subprocess
import time
import sys
import os.path

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

def process():
	try:

		conn = getDBConnection()

		cursor = conn.cursor()
		cursor.execute("SELECT * FROM queries where execution_status = 'INITIATED' ORDER BY id LIMIT 5")
		all_query_result = cursor.fetchall()
		cursor.close()

		if len(all_query_result):
			length = 0
			while length < len(all_query_result):
				query_result = all_query_result[length]
				request_file_path = query_result[1]

				cursor = conn.cursor()
				cursor.execute("UPDATE queries SET execution_status='PENDING', executed_at='%s' WHERE id=%s" % (datetime.now(), query_result[0]))
				conn.commit()
				cursor.close()

				query_file = open(request_file_path)
				data = json.load(query_file)
				query = data['query']
				query_file.close()

				spark_config_file = open(os.getcwd()+"/configs/spark.config.json")
				spark_config_data = json.load(spark_config_file)
				spark_config_file.close()
				
				spark_master = spark_config_data['spark_master']
				submit_application_path = spark_config_data['submit_apllication_path']
				conf = spark_config_data['spark_conf']

				spark_submit_str = ('spark-submit '
									'--master ' + spark_master + ' '
									'--jars ' + submit_application_path + 'jars/spark-cassandra-connector-assembly-3.1.0-22-g5142a4c6.jar '
									'--py-files ' + submit_application_path + 'packages.zip '
									'--files ' + submit_application_path + 'configs/job_config.json' + ',' + request_file_path + ' ')

				for key, value in conf.items() :
					spark_submit_str+= ('--conf ' + key + '=' + value)

				spark_submit_str += ' ' + submit_application_path + 'jobs/job.py' + ' ' + str(query_result[0])

				process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
				stdout,stderr = process.communicate()
				# if process.returncode !=0:
				#    print(stderr)
				# print(stdout)
				length = length + 1			
		cursor.close()
		conn.close()

	except Exception as e:
		print(e)

def set_interval(func, sec): 
    def func_wrapper():
    	func()
    	set_interval(func, sec)  
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


if __name__ == "__main__":
	
	set_interval(process, 5)

