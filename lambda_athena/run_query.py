import time
from common import common

def run_query(session, DB, query, value=False):
    start = time.time()
    queryResult = dml_query_results(session, query, DB)
    runtime = time.time() - start
    print("{}, {} : {}".format(str(runtime), str(queryResult), query))
    if (queryResult == False):
        raise Exception(query)
    # if True, return query result rows
    if (value == True):
        return queryResult

def dml_query_results(session, query, database, wait = True):
    location, result = query_results(session, query, database, wait)
    return result

def get_var_char_values(d):
    return [obj['VarCharValue'] for obj in d['Data']]

def query_results(session, query, database, wait=True):
    client = session.client('lambda_athena')
    print(query)
    # this function executes the query and returns the query execution ID
    response_query_execution_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': common.ATHENA_OUTPUT_LOCATION
        }
    )

    if not wait:
        return response_query_execution_id['QueryExecutionId'], True
    else:
        response_get_query_details = client.get_query_execution(
            QueryExecutionId=response_query_execution_id['QueryExecutionId']
        )
        status = 'RUNNING'
        iterations = 360
        while (iterations > 0):
            iterations = iterations - 1
            response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id['QueryExecutionId']
            )
            status = response_get_query_details['QueryExecution']['Status']['State']

            if (status == 'FAILED') or (status == 'CANCELLED'):
                print(response_get_query_details)
                return False, False

            elif status == 'SUCCEEDED':
                location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                # function to get output results
                response_query_result = client.get_query_results(
                    QueryExecutionId=response_query_execution_id['QueryExecutionId']
                )
                print(response_query_result)
                result_data = response_query_result['ResultSet']

                if len(response_query_result['ResultSet']['Rows']) > 1:
                    header = response_query_result['ResultSet']['Rows'][0]
                    rows = response_query_result['ResultSet']['Rows'][1:]
                    header = [obj['VarCharValue'] for obj in header['Data']]
                    result = [dict(zip(header, get_var_char_values(row))) for row in rows]
                    return location, result
                else:
                    return location, None
            else:
                time.sleep(5)

        return None, False