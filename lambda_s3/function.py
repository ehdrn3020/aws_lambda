from common import common

def get_s3_file_list(session, bucket, table, target_date):
    s3 = session.client('s3')
    file_list = []

    try:
        list_obj = s3.list_objects(Bucket=common.BUCEKT, Prefix=f's3://{bucket}/{table}/{target_date}')['Contents']
        for path in list_obj:
            dt = path['Key'].split('/')[-2] # date='2022-04-19'
            if f'date={target_date}' == dt:
                file_list.append(path['Key'])
    # Contents가 null일때
    except KeyError as e:
        print(f's3 file is none, table:{table}, {e}')

    return file_list

def put_s3_file(session, bucket, table, target_date):
    s3 = session.client('s3')
    prefix = f's3://{bucket}/{table}/{target_date}/_TESTFILE'
    s3.put_object(Bucket=common.DIGHTY_ASP_BUCKET, Key=prefix)

def delete_s3_file(session, bucket, path):
    s3 = session.resource('s3')
    bucket_name = s3.Bucket(bucket)
    print("DELETE S3 : s3://{}/{}".format(bucket, path))

    for obj in bucket_name.objects.filter(Prefix=path):
        s3.Object(bucket_name.name, obj.key).delete()