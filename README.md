pyhton	3.8
pip	20.3.4
pipenv	2021.5.29
py4j	0.10.9.5

./spark-submit \
--master spark://tamjid-Lenovo-B41-80:7077 \
--jars /home/tamjid/Work/big_data_report/submit/jars/spark-cassandra-connector-assembly-3.1.0-22-g5142a4c6.jar \
--py-files /home/tamjid/Work/big_data_report/submit/packages.zip \
--files /home/tamjid/Work/big_data_report/submit/configs/job_config.json,/home/tamjid/Work/big_data_report/request/1_request.json \
--conf spark.cassandra.connection.host="127.0.0.1" \
/home/tamjid/Work/big_data_report/submit/jobs/job.py 1

./spark-submit \
--master spark://tamjid-Lenovo-B41-80:7077 \
--jars /home/tamjid/Work/big_data_report/submit/jars/spark-cassandra-connector-assembly-3.1.0-22-g5142a4c6.jar,/home/tamjid/Work/big_data_report/submit/jars/mysql-connector-java-8.0.28.jar \
--py-files /home/tamjid/Work/big_data_report/submit/packages.zip \
--files /home/tamjid/Work/big_data_report/submit/configs/job_config.json,/home/tamjid/Work/big_data_report/request/2_request.json \
--conf spark.cassandra.connection.host="127.0.0.1" \
/home/tamjid/Work/big_data_report/submit/jobs/job.py 5

./spark-submit \
--master spark://tamjid-Lenovo-B41-80:7077 \
--jars /home/tamjid/Work/big_data_report/submit/jars/spark-cassandra-connector-assembly-3.1.0-22-g5142a4c6.jar,/home/tamjid/Work/big_data_report/submit/jars/spark-filetransfer_2.12-0.3.0.jar \
--py-files /home/tamjid/Work/big_data_report/submit/packages.zip \
--files /home/tamjid/Work/big_data_report/submit/configs/job_config.json,/home/tamjid/Work/big_data_report/request/1_request.json \
--conf spark.cassandra.connection.host="127.0.0.1" \
/home/tamjid/Work/big_data_report/submit/jobs/job.py 1

create table

CREATE TABLE `queries` (
  `id` int NOT NULL AUTO_INCREMENT,
  `request_file_path` varchar(256) NOT NULL,
  `execution_status` enum('INITIATED','PENDING','SUCCESS','FAILED') DEFAULT 'INITIATED',
  `callback` varchar(256) DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `executed_at` datetime DEFAULT NULL,
  `response_file_path` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `file_path_UNIQUE` (`request_file_path`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `big_data_report`.`load_test_latency` (
  `attack_type` VARCHAR(45) NOT NULL,
  `average_incoming_traffic` BIGINT NOT NULL,
  `attack_protocol` VARCHAR(45) NULL DEFAULT NULL,
  `average_incoming_flow` INT NULL DEFAULT 0,
  `average_incoming_pps` INT NULL DEFAULT 0,
  `ip` VARCHAR(20) NOT NULL,
  PRIMARY KEY (`id`));

