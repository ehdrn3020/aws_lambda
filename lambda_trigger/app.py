import json
import os
import sys
import urllib
import boto3

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from common import send_sns

session = boto3.Session()

def lambda_handler(event, context):
    try:

        # get s3 trigger info
        bucket = event['Records'][0]['s3']['bucket']['name'] # my-bucket
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') # my-table/dt=2022-04-19/file.parquet

        # execute lambda
        body = {"user_no": 1, "user_age": 45,
                "user_info": [{"region": "seoul", "phone": "123-1231-2312"}]}
        lmb = session.client(service_name='lambda', region_name="ap-northeast-2")
        lmb.invoke(FunctionName="your_lambda_function", InvocationType='Event', Payload=json.dumps(body))
        ## get data other lambda
        # user_no = event.get('user_no', 0)

        # set glue trigger
        gle = session.client('glue')
        run_gle = gle.start_job_run(JobName="your_glue_function")
        get_gle = gle.get_job_run(JobName="your_glue_function", RunId=run_gle['JobRunId'])
        print("Job Status : ", get_gle['JobRun']['JobRunState'])

        return {
            'statusCode': 200,
            'body': json.dumps('Succuess')
        }

    except Exception as e:
        print(e)
        send_sns("lambda trigger:", e)
        raise e