from dependencies.spark import start_spark
from dependencies.notify import notify
import os
import shutil
import sys

def extract_transform_data(spark, request):
	table = request['structure']['FROM']['table']
	df = []

	if table:
		for x in table:
			load_options = { "table": x, "keyspace": request['structure']['FROM']['keyspace']}
			df = spark.read.format("org.apache.spark.sql.cassandra").options(**load_options).load()
			df.createTempView(x)
		transformed_data=spark.sql(request['query'])
		return transformed_data
	else:
		return None

# def transform_data(spark, df, request):
# 	df.createTempView(request['structure']['FROM']['table'])
# 	transformed_data=spark.sql(request['query'])
# 	return transformed_data

def load_data(df, config, request):
	TEMPORARY_TARGET=config['destination_path']['file']['path'] + str(request['id']) + config['destination_path']['file']['postfix_name']
	#TEMPORARY_TARGET=config['destination_path']['file']['ftp']
	DESIRED_TARGET=config['destination_path']['file']['path'] + str(request['id']) + config['destination_path']['file']['postfix_name'] + '.csv'

	(df
	.coalesce(1)
	.write
	.csv(TEMPORARY_TARGET, mode='overwrite', header=True))

	part_filename = next(entry for entry in os.listdir(TEMPORARY_TARGET) if entry.startswith('part-'))
	temporary_csv = os.path.join(TEMPORARY_TARGET, part_filename)

	shutil.copyfile(temporary_csv, DESIRED_TARGET)
	shutil.rmtree(TEMPORARY_TARGET)
	return config['destination_path']['file']['file_server_path'] + str(request['id']) + config['destination_path']['file']['postfix_name'] + '.csv';

	# (df.write.mode("overwrite")
	# 	.format("jdbc")
	# 	.option("url", "jdbc:mysql://localhost:3306/big_data_report")
	# 	.option("driver", "com.mysql.cj.jdbc.Driver")
	# 	.option("dbtable", "load_test_latency")
	# 	.option("user", "root")
	# 	.option("password", "root")
	# 	.save())

	# return "save to mysql"

	# (df.write
 #    .format(TEMPORARY_TARGET['format'])
 #    .option("protocol", TEMPORARY_TARGET['protocol'])
 #    .option("host", TEMPORARY_TARGET['host'])
 #    .option("port", TEMPORARY_TARGET['port'])
 #    .option("username", TEMPORARY_TARGET['username'])
 #    .option("password", TEMPORARY_TARGET['password'])
 #    .option("fileFormat", TEMPORARY_TARGET['fileFormat'])
 #    .save(TEMPORARY_TARGET['path'] + str(request['id']) + TEMPORARY_TARGET['postfix_name']))
	# (df
 #  .coalesce(1)
 #  .write
 #  .csv(config['destination_path']['file']['test_ftp_file_path'], mode='overwrite', header=True))
	#return "Success: save to file path"

def main():
	requestId = sys.argv[1]
	request_file_path = '/home/tamjid/Work/big_data_report/request/'+requestId+'_'+'request.json'
	spark, config, request = start_spark(
    	app_name='spark_cassandra_job',
    	files=['/home/tamjid/Work/big_data_report/submit/configs/job_config.json', request_file_path]
 	)
	data = extract_transform_data(spark, request)
	# transformed_data = transform_data(spark, data, request)
	file_path = load_data(data, config, request)
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