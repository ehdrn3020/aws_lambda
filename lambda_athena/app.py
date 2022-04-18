import json
import sys
import os.path
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from . import function
from common import common
from common import send_sns

# set global variable
session = boto3.Session()
KST = pytz.timezone('Asia/Seoul')
db = common.DB
table = common.TABLE
bucket = common.BUCEKT

def lambda_handler(event, context):
    try:
        # set datetime KST(UTC+9)
        now = datetime.now(KST)
        target_date = (now - relativedelta(days=1)).strftime('%Y-%m-%d')

        # select table
        query_result = function.select_table(session, db, table, target_date)
        for row in query_result:
            key_column = row['column1']+'_'+row['column2']
            print("get column1+2: ", key_column)

        # create table
        table_columns = {
            'users': {
                'column': ['name', 'age', 'position', 'salary'],
                'type': ['string', 'integer', 'string', 'double']
            }
        }
        partition = "`user_no` bigint,`user_account` string"
        str_columns = ''
        for idx, column in enumerate(table_columns['users']['column']):
            type = table_columns[table]['type'][idx]
            str_columns += '`{}` {},'.format(column, type)
        str_columns = str_columns[0:-1]
        function.create_table(session, db, table, target_date, str_columns, partition)

        # drop lambda_athena table
        function.drop_table(session, db, table)

        # alter set s3 location
        s3_path = f's3://your-bucket/{table}/{target_date}'
        function.set_location_with_path(session, db, table, s3_path)

        # add & drop partition
        partition = f"user_no='99845326', user_account='123-11020-2022'"
        function.add_partition(session, db, table, partition)
        function.drop_partition(session, db, table, partition)

        # after input date s3 file to s3 path, execution
        # relocate partitions
        function.msck_repair_table(session, db, table)

        return {
            'statusCode': 200,
            'body': json.dumps('Succuess')
        }

    except Exception as e:
        print(e)
        send_sns("lambda athean:", e)
        raise e