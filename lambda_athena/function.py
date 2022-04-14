from . import common
from . import run_query

def select_table(session, db, table, target_date):
    query = """
            select *
            from {}.{} 
            where date='{}'
            limit 30;
            """.format(db, table, target_date)
    return run_query(session, db, query, True)

def create_table(session, db, table, target_date, str_columns, partition):
    query = """
        CREATE EXTERNAL TABLE `{}.{}`({})
        PARTITIONED BY ({})
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION 's3://my-bucket/{}/{}'""".format(db, table, str_columns, partition, table, target_date)
    run_query(session, db, query)

def drop_table(session, db, table):
    query = "drop table if exists {}.{}".format(db, table)
    run_query(session, db, query)

def set_location_with_path(session, db, table, s3_path):
    query = "alter table {}.{} set location '{}'".format(db, table, s3_path)
    run_query(session, db, query)

def msck_repair_table(session, db, table):
    query = "MSCK REPAIR TABLE {}.{}".format(db, table)
    run_query(session, db, query)

def add_partition(session, db, table, partition):
    query = "alter table {}.{} add IF NOT EXISTS partition ({})".format(db, table, partition)
    run_query(session, db, query)

def drop_partition(session, db, table, partition):
    query = "alter table {}.{} drop IF EXISTS partition ({})".format(db, table, partition)
    run_query(session, db, query)