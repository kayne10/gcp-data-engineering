template_name="flights_etl"
cluster_name="spark-job-flights"
current_date="2019-05-05"
bucket="gs://etl-practice"

gcloud dataproc workflow-templates delete -q $template_name --region "us-east1" &&

gcloud beta dataproc workflow-templates create $template_name --region "us-east1" &&

gcloud beta dataproc workflow-templates set-managed-cluster $template_name --region "us-east1" \
--cluster-name=$cluster_name \
 --scopes=default \
 --master-machine-type n1-standard-2 \
 --master-boot-disk-size 20 \
  --num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 20 \
--image-version 1.3 &&

gcloud dataproc workflow-templates \
 add-job pyspark $bucket/spark-job/flights-etl.py \
--step-id flight_delays_etl \
--workflow-template=$template_name \
--region "us-east1" &&

gcloud beta dataproc workflow-templates instantiate $template_name --region "us-east1" && 

bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_distance \
 $bucket/flights-data-output/${current_date}"_distance_category/*.json" &&

 bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_flight_nums \
 $bucket/flights-data-output/${current_date}"_flight_nums/*.json"


