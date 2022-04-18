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
        # get trigger
        bucket = event['Records'][0]['s3']['bucket']['name']

        # get s3 list objects

        # put s3 file

        # delte s3 file

        return {
            'statusCode': 200,
            'body': json.dumps('Succuess')
        }

    except Exception as e:
        print(e)
        send_sns("lambda s3:", e)
        raise e