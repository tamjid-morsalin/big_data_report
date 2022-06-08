pyhton	3.8
pip	20.3.4
pipenv	2021.5.29
py4j	0.10.9.5

./spark-submit \
--master spark://tamjid-Lenovo-B41-80:7077 \
--jars /home/tamjid/Work/big_data_report/submit/jars/spark-cassandra-connector-assembly-3.1.0-22-g5142a4c6.jar \
--py-files /home/tamjid/Work/big_data_report/submit/packages.zip \
--files /home/tamjid/Work/big_data_report/submit/configs/job_config.json,/home/tamjid/Work/big_data_report/request/2_request.json \
--conf spark.cassandra.connection.host="127.0.0.1" \
/home/tamjid/Work/big_data_report/submit/jobs/job.py