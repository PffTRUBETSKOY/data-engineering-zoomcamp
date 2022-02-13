

CREATE OR REPLACE TABLE key-sign-338616.trips_data_all.yellow_tripdata_partitioned PARTITION BY date(tpep_pickup_datetime) AS SELECT * from key-sign-338616.trips_data_all.external_yellow_tripdata;

CREATE OR REPLACE EXTERNAL TABLE key-sign-338616.trips_data_all.external_fhv_tripdata_2019 OPTIONS(format = 'parquet', uris= ['gs://dtc_data_lake_key-sign-338616/raw/output_fhv_2019-*.parquet']);

select count(*) from key-sign-338616.trips_data_all.external_fhv_tripdata_2019;

select count(distinct dispatching_base_num )from key-sign-338616.trips_data_all.external_fhv_tripdata_2019;

CREATE OR REPLACE TABLE key-sign-338616.trips_data_all.fhv_tripdata_2019_part_clust PARTITION BY date(dropoff_datetime) CLUSTER BY (dispatching_base_num) AS SELECT * from key-sign-338616.trips_data_all.external_fhv_tripdata_2019;

select count(*) from key-sign-338616.trips_data_all.fhv_tripdata_2019_part_clust where (dispatching_base_num = 'B00987' or dispatching_base_num = 'B02060' or dispatching_base_num = 'B02279') and date(dropoff_datetime) between '2019-01-01' and '2019-03-31';

select * from key-sign-338616.trips_data_all.external_fhv_tripdata_2019 limit 10;
