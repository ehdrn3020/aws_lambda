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
            print("get column1+2: ",key_column)

        # create table
        table_columns = {
            'users': {
                'column': ['name', 'age', 'position', 'salary'],
                'type': ['string', 'integer', 'string', 'double']
            }
        }
        partition = "`user_no` string,`shop_no` bigint"
        PARTITION_HISTORY = "`mall_id` string, `shop_no` bingint, `dt` string"
        str_columns = ''
        for idx, column in enumerate(table_columns['users']['column']):
            type = table_columns[table]['type'][idx]
            str_columns += '`{}` {},'.format(column, type)
        function.create_table(session, db, table, target_date, str_columns, partition)
        # drop lambda_athena table
        function.drop_table()
        return {
            'statusCode': 200,
            'body': json.dumps('Succuess')
        }

    except Exception as e:
        print(e)
        send_sns("lambda athean:", e)
        raise e