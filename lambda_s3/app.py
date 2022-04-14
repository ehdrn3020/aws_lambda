import json
import sys
import os.path
import pytz

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from common import common
from common import send_sns

KST = pytz.timezone('Asia/Seoul')
db = common.DB
table = common.TABLE
bucket = common.BUCEKT

def lambda_handler(event, context):
    try:


        # create athena table

        # drop lambda_athena table

        return {
            'statusCode': 200,
            'body': json.dumps('Succuess')
        }

    except Exception as e:
        print(e)
        send_sns("lambda athean:", e)
        raise e