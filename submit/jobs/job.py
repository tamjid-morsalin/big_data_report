from dependencies.spark import start_spark
from dependencies.notify import notify
import os
import shutil
import sys

def extract_data(spark, request):
	load_options = { "table": request['structure']['FROM']['table'], "keyspace": request['structure']['FROM']['keyspace']}
	df=spark.read.format("org.apache.spark.sql.cassandra").options(**load_options).load()
	return df

def transform_data(spark, df, request):
	df.createTempView(request['structure']['FROM']['table'])
	transformed_data=spark.sql(request['query'])
	return transformed_data

def load_data(df, config, request):
	TEMPORARY_TARGET=config['destination_path']['file']['path'] + str(request['id']) + config['destination_path']['file']['postfix_name']
	DESIRED_TARGET=config['destination_path']['file']['path'] + str(request['id']) + config['destination_path']['file']['postfix_name'] + '.csv'

	#file_name = config['destination_path']['file']['path'] + str(request['id']) + config['destination_path']['file']['postfix_name']
	(df
  .coalesce(1)
  .write
  .csv(TEMPORARY_TARGET, mode='overwrite', header=True))

	part_filename = next(entry for entry in os.listdir(TEMPORARY_TARGET) if entry.startswith('part-'))
	temporary_csv = os.path.join(TEMPORARY_TARGET, part_filename)

	shutil.copyfile(temporary_csv, DESIRED_TARGET)
	shutil.rmtree(TEMPORARY_TARGET)
	return config['destination_path']['file']['file_server_path'] + str(request['id']) + config['destination_path']['file']['postfix_name'] + '.csv';

def main():
	requestId = sys.argv[1]
	request_file_path = '/home/tamjid/Work/big_data_report/request/'+requestId+'_'+'request.json'
	spark, config, request = start_spark(
    	app_name='spark_cassandra_job',
    	files=['/home/tamjid/Work/big_data_report/submit/configs/job_config.json', request_file_path]
 	)
	data = extract_data(spark, request)
	transformed_data = transform_data(spark, data, request)
	file_path = load_data(transformed_data, config, request)
	spark.stop()

	notify(
		data=[file_path],
		config=config,
		request=request
	)

	return None

# entry point for PySpark ETL application
if __name__ == '__main__':
	main()