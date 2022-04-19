import json
import sys
import os.path
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from common import common
from common import send_sns
from . import function

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

        # get s3 list objects, filtered date
        function.get_s3_file_list(session, bucket, table, target_date)

        # put s3 file
        function.put_s3_file(session, bucket, table, target_date)

        # delete s3 file

        # s3 retention

        return {
            'statusCode': 200,
            'body': json.dumps('Succuess')
        }

    except Exception as e:
        print(e)
        send_sns("lambda s3:", e)
        raise e