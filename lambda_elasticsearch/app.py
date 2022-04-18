import json
import sys
import os.path
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from . import function
from common import send_sns

# set global variable
session = boto3.Session()
KST = pytz.timezone('Asia/Seoul')

def lambda_handler(event, context):
    try:
        # set datetime KST(UTC+9)
        now = datetime.now(KST)
        target_date = (now - relativedelta(days=1)).strftime('%Y-%m-%d')

        # put data to elasticsearch
        function.put_elasticsearch(session, 'my_db', "user_table", target_date, 'daily_log_table', "dt = '{}'".format(target_date), 'log_users')

        return {
                'statusCode': 200,
                'body': json.dumps('Succuess')
            }

    except Exception as e:
        print(e)
        send_sns("lambda elk:", e)
        raise e