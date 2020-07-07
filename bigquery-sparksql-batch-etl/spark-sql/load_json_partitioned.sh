bucket=gs://sid-etl

current_date=$(date +"%Y-%m-%d")


bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_distance \
 $bucket/flights-data-output/${current_date}"_distance_category/*.json" &&


bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_flight_nums \
 $bucket/flights-data-output/${current_date}"_flight_nums/*.json"


#my use case since I am doing this the next day
bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_distance \
 $bucket/flights-data-output/2020-07-06"_distance_cat/*.json"

bq load --source_format=NEWLINE_DELIMITED_JSON \
 data_analysis.avg_delays_by_flight_nums \
 $bucket/flights-data-output/2020-07-06"_flight_nums/*.json"