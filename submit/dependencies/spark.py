import __main__
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkFiles
import json
from os import environ, listdir, path

def start_spark(app_name='spark_cassandra_app', jar_packages=[], files=[], spark_config={}, additional_params={}):
	spark_builder = (
		SparkSession
		.builder
		.appName(app_name)
	)

	spark_jar_packages = ','.join(list(jar_packages))
	spark_builder.config('spark.jars.packages', spark_jar_packages)

	spark_files = ','.join(list(files))
	spark_builder.config('spark.files', spark_files)

	for key, val in spark_config.items():
		spark_builder.config(key, val)

	spark_session = spark_builder.getOrCreate()

	spark_files_dir = SparkFiles.getRootDirectory()

	config_files = [filename
					for filename in listdir(spark_files_dir)
					if filename.endswith("config.json")]
	if config_files:
		path_to_config_file = path.join(spark_files_dir, config_files[0])
		with open(path_to_config_file, 'r') as config_file:
			config_dict = json.load(config_file)
	else:
		config_dict = None

	# request_file_name = additional_params['id']+"_"+"request.json"
	request_files = [filename
					for filename in listdir(spark_files_dir)
					if filename.endswith("request.json")]
	if request_files:
		path_to_request_file = path.join(spark_files_dir, request_files[0])
		with open(path_to_request_file, 'r') as request_file:
			request_dict = json.load(request_file)
	else:
		request_dict = None

	return spark_session, config_dict, request_dict