import boto3

def get_cross_iam_role(event, context):

    sts_connection = boto3.client('sts')
    acct_b = sts_connection.assume_role(
        # set remote role you want
        RoleArn="arn:aws:iam::123412341234:role/service-role/another-lambda-role",
        RoleSessionName="cross_acct_lambda"
    )

    ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
    SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
    SESSION_TOKEN = acct_b['Credentials']['SessionToken']

    # create service client using the assumed role credentials, e.g. S3
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN,
    )

    return session

    # precede work
    # https://aws.amazon.com/ko/premiumsupport/knowledge-center/lambda-function-assume-iam-role/?nc1=h_ls
