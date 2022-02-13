<!-- Create the partitioned yellow tripdata -->
CREATE OR REPLACE TABLE key-sign-338616.trips_data_all.yellow_tripdata_partitioned
PARTITION BY date(tpep_pickup_datetime) AS 
SELECT * from key-sign-338616.trips_data_all.external_yellow_tripdata;

<!-- Load only 2019 data to the external table -->
CREATE OR REPLACE EXTERNAL TABLE key-sign-338616.trips_data_all.external_fhv_tripdata_2019
OPTIONS(format = 'parquet',
uris= ['gs://dtc_data_lake_key-sign-338616/raw/output_fhv_2019-*.parquet']);

<!-- Count the number of records in 2019 from external table -->
select count(*) from key-sign-338616.trips_data_all.external_fhv_tripdata_2019;

<!-- Count dspatching bases operating in 2019 -->
select count(distinct dispatching_base_num )from key-sign-338616.trips_data_all.external_fhv_tripdata_2019;

<!-- Create the clustered and partitioned 2019 data -->
CREATE OR REPLACE TABLE key-sign-338616.trips_data_all.fhv_tripdata_2019_part_clust
PARTITION BY date(dropoff_datetime)
CLUSTER BY (dispatching_base_num) AS
SELECT  * from key-sign-338616.trips_data_all.external_fhv_tripdata_2019;

<!-- count the number of trips on first 3 months of 2019 from 3 specific bases -->
select count(*) from key-sign-338616.trips_data_all.fhv_tripdata_2019_part_clust
where (dispatching_base_num = 'B00987' or dispatching_base_num = 'B02060' or dispatching_base_num = 'B02279') 
and date(dropoff_datetime) between '2019-01-01' and '2019-03-31';

<!-- Check what values are present for SR_Flag -->
select * from key-sign-338616.trips_data_all.external_fhv_tripdata_2019 limit 10;