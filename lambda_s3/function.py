from common import common

def get_s3_file_list(session, bucket, table, target_date):
    s3 = session.client('s3')
    file_list = []

    try:
        list_obj = s3.list_objects(Bucket=common.BUCEKT, Prefix=f's3://{bucket}/{table}/{target_date}')['Contents']
        for path in list_obj:
            dt = path['Key'].split('/')[-2] # dt='2022-04-19'
            if f'dt={target_date}' == dt:
                file_list.append(path['Key'])
    # Contents가 null일때
    except KeyError as e:
        print(f's3 file is none, key:{key}, {e}')

    return file_list

def put_s3_file(session, bucket, table, target_date):
    s3 = session.client('s3')
    prefix = f's3://{bucket}/{table}/{target_date}/_TESTFILE'
    s3.put_object(Bucket=common.DIGHTY_ASP_BUCKET, Key=prefix)