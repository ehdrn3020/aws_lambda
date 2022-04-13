import json
from common import common
# import requests


def lambda_handler(event, context):
    BUCKET = common.MASTER_FOLDER

    # create lambda_athena table

    # drop lambda_athena table

    return {
        'statusCode': 200,
        'body': json.dumps('Succuess')
    }

