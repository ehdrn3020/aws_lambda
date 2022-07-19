import json
import sys
import boto3
import urllib
import os.path
import importlib
import time
import logging

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from lambda_athena import function
from common.s3_cleanup import clean_up
from common import send_sns
from common import common

# set global variable
session = boto3.Session()
KST = pytz.timezone('Asia/Seoul')
db = common.DB
table = common.TABLE
bucket = common.BUCEKT

TARGET = ['table_1', 'table_2']
retry = 10

def lambda_handler(event, context):

    try:
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        # key = 'all/service_name/myservice/call_count/partition_1=korea/partition_2=1/dt=20220714/trigger_file'

        dt = key.split('/')[-2].replace('dt=', '')
        partition_2 = key.split('/')[-3].replace('partition_2=', '')
        partition_1 = key.split('/')[-4].replace('partition_1=', '')

        for target in TARGET:
            for retry_cnt in range(retry):
                try:
                    # drop lambda_athena table
                    function.drop_table(session, db, target)
                    # alter set s3 location
                    s3_path = f's3://your-bucket/{target}/{dt}'
                    function.set_location_with_path(session, db, target, s3_path)
                    break

                except Exception as e:
                    # retry specific exception
                    if e.response['Error']['Code'] == 'TooManyRequestsException':
                        # limit retry count
                        if retry_cnt < 10:
                            print(f"TooManyRequestsException is Rised, Retry Count is :{retry_cnt+1}")
                            logging.info("TooManyRequestsException log")
                            time.sleep(3)
                            continue
                        raise e
                    print(e)
                    send_sns("lambda exception:", e)
                    raise e

        return {
            'statusCode': 200,
            'body': json.dumps('Success!')
        }

    except Exception as e:
        print(e)
        send_sns("lambda exception:", e)
        raise e
