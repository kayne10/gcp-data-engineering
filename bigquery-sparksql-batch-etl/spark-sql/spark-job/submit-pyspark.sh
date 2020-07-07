gcloud dataproc jobs submit pyspark \
gs://etl-practice/spark-job/flights-etl.py --cluster=spark-dwh --region=us-east1 