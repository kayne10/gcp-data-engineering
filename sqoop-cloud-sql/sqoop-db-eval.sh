#!/bin/bash

bucket="gs://etl-practice"

pwd_file=$bucket/sqoop-pwd/pwd.txt

cluster_name="ephemeral-spark-cluster-20190518"

gcloud dataproc jobs submit hadoop \
--cluster=$cluster_name --region=asia-east1 \
--class=org.apache.sqoop.Sqoop \
--jars=$bucket/sqoop-jars/sqoop_sqoop-1.4.7.jar,$bucket/sqoop-jars/sqoop_avro-tools-1.8.2.jar,file:///usr/share/java/mysql-connector-java-5.1.42.jar \
-- import \
-Dmapreduce.job.user.classpath.first=true \
--driver com.mysql.jdbc.Driver \
--connect="jdbc:mysql://localhost:3307/airports" \
--username=root --password-file=$pwd_file \
--query "select count(*) from flights limit 10"
