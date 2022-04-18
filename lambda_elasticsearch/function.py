import requests
import time
import json
from common import common
from lambda_athena import run_query

def put_elasticsearch(session, db, table, date, type, condition='', name='', columns='count(distinct user_no) as count'):
    count = get_count_query(session, db, table, condition, columns)
    if name != "":
        table = name
    request_elasticsearch(table, date, type, count)

def get_count_query(session, db, target_table, condition, columns):
    if condition != "":
        condition = ' where {}'.format(condition)
    query = """
        SELECT
            {}
        FROM
             {}.{}
        {}
    """.format(columns, db, target_table, condition)

    start = time.time()
    location, results = run_query.query_results(session, query, db)
    runtime = time.time() - start
    if len(results) > 0:
        print("{} : {} : {}".format(str(runtime), location, results[0].get('count')))
        return results[0].get('count')
    return 0

def request_elasticsearch(table, date, type, count):
    name = "{}_{}".format(table, type)
    headers = {'Content-Type': 'application/json'}
    url = common.ES_URL.format(
        date[:6], "{}-{}".format(name, date))
    json_data = {'name': '{}_{}'.format(table, type), 'src': '{}'.format(table), 'count_date': '{}'.format(date),
                 'count': count, 'type': '{}'.format(type)}
    print("{} : {}".format(url, json_data))

    result = requests.put(url, auth=('elk_web_login_id', 'elk_web_login_password'), headers=headers, data=json.dumps(json_data), verify=False)
    print(result.text)