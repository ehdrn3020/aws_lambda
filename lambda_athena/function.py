from . import common
from . import run_query

def select_date():

def create_table(session, DB, table, target_date, str_columns, partition):
    query = """
        CREATE EXTERNAL TABLE `{}.{}`({})
        PARTITIONED BY ({})
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION 's3://my-bucket/{}/{}'""".format(DB, table, str_columns, partition, table, target_date)
    run_query(session, DB, query)

def drop_table(session, DB, table):
    query = "drop table if exists {}.{}".format(DB, table)
    run_query(session, DB, query)

def set_location_with_path(session, DB, table, s3_path):
    query = "alter table {}.{} set location '{}'".format(DB, table, s3_path)
    run_query(session, DB, query)

def msck_repair_table(session, DB, table):
    query = "MSCK REPAIR TABLE {}.{}".format(DB, table)
    run_query(session, DB, query)

def add_partition(session, DB, table, partition):
    query = "alter table {}.{} add IF NOT EXISTS partition ({})".format(DB, table, partition)
    run_query(session, DB, query)

def drop_partition(session, DB, table, partition):
    query = "alter table {}.{} drop IF EXISTS partition ({})".format(DB, table, partition)
    run_query(session, DB, query)