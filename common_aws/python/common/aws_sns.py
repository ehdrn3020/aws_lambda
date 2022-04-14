import boto3
import json
import logging
from . import common

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set up Boto 3 client for SNS
client = boto3.client('sns')

# Define Lambda function
def send_sns(function_name, emessage, topic=common.SNS_TOPIC_ARN):
    message = """
        Error function :{}
        Error Message.: {}
    """.format(function_name, emessage)
    print(message)
    response = client.publish(
        TargetArn=topic,
        Message=json.dumps({'default': json.dumps(message)}),
        Subject='An AWS Lambda has failed : ' + function_name,
        MessageStructure='json')