# Project Number: 703112408605

from googleapiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
import httplib2
import pprint
from googleapiclient.errors import HttpError


def main():
    # Enter your Google Developer Project number
    PROJECT_NUMBER = '703112408605'
    DATASET_ID = 'pete_test_dataset'
    TABLE_ID = 'pete_test_table'
    TARGET_TABLE_ID = 'pete_test_target'

    FLOW = flow_from_clientsecrets('../../conf/client_secrets.json', scope='https://www.googleapis.com/auth/bigquery')

    storage = Storage('bigquery_credentials.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(FLOW, storage, tools.argparser.parse_args([]))

    http = httplib2.Http()
    http = credentials.authorize(http)

    bigquery_service = build('bigquery', 'v2', http=http)


    # Check if the table exists, if yes DELETE IT!
    delete_table(bigquery_service, PROJECT_NUMBER, DATASET_ID, TABLE_ID)

    # Create the table
    table_data = {
        "friendlyName": "python test table",  # [Optional] A descriptive name for this table.
        "schema": {  # [Optional] Describes the schema of this table.
                     "fields": [  # Describes the fields in a table.
                         {"type": "STRING", "name": "department"},
                         {"type": "STRING", "name": "entity"},
                         {"type": "STRING", "name": "payment_date"},
                         {"type": "STRING", "name": "transaction"},
                         {"type": "STRING", "name": "invoice_amount"},
                         {"type": "STRING", "name": "supplier"},
                         {"type": "STRING", "name": "description"},
                     ],
        },
        "tableReference": {  # [Required] Reference describing the ID of this table.
                             "projectId": PROJECT_NUMBER,  # [Required] The ID of the project containing this table.
                             "tableId": TABLE_ID,
                             "datasetId": DATASET_ID,  # [Required] The ID of the dataset containing this table.
        },
    }

    # Create the table
    bigquery_service.tables().insert(projectId=PROJECT_NUMBER, datasetId=DATASET_ID, body=table_data).execute()

    # Next load the data just as string
    jobData = {
                'projectId': PROJECT_NUMBER,
                'configuration': {
                    'load': {
                        'sourceUris': ['gs://ukfco-public-spend/Publishable_September_2014_Spend.csv'],
                        'schema': {
                            "fields": [
                                 {"type": "STRING", "name": "department"},
                                 {"type": "STRING", "name": "entity"},
                                 {"type": "STRING", "name": "payment_date"},
                                 {"type": "STRING", "name": "transaction"},
                                 {"type": "STRING", "name": "invoice_amount"},
                                 {"type": "STRING", "name": "supplier"},
                                 {"type": "STRING", "name": "description"},
                            ],
                        },
                        'destinationTable': {
                            'projectId': PROJECT_NUMBER,
                            'datasetId': DATASET_ID,
                            'tableId': TABLE_ID
                        },
                        "skipLeadingRows": 1,
                    }
                }
            }
    load_table(bigquery_service, PROJECT_NUMBER, jobData)

    # TODO: Transform the STRING values into TIMESTAMP and FLOAT values


    # Create the post transform target table
    delete_table(bigquery_service, PROJECT_NUMBER, DATASET_ID, TARGET_TABLE_ID)
    target_table_data = {
        "friendlyName": "python target test table",  # [Optional] A descriptive name for this table.
        "schema": {  # [Optional] Describes the schema of this table.
                     "fields": [  # Describes the fields in a table.
                         {"type": "STRING", "name": "department"},
                         {"type": "STRING", "name": "entity"},
                         {"type": "TIMESTAMP", "name": "payment_date_clean"},
                         {"type": "FLOAT", "name": "transaction_clean"},
                         {"type": "FLOAT", "name": "invoice_amount_clean"},
                         {"type": "STRING", "name": "supplier"},
                         {"type": "STRING", "name": "description"},
                     ],
        },
        "tableReference": {  # [Required] Reference describing the ID of this table.
                             "projectId": PROJECT_NUMBER,  # [Required] The ID of the project containing this table.
                             "tableId": TARGET_TABLE_ID,
                             "datasetId": DATASET_ID,  # [Required] The ID of the dataset containing this table.
        },
    }
    bigquery_service.tables().insert(projectId=PROJECT_NUMBER, datasetId=DATASET_ID, body=target_table_data).execute()


    # Insert a query job that implements the following SQL:
    munging_query = '''
        SELECT department,
           entity,
           TIMESTAMP(CONCAT(SUBSTR(payment_date, 7, 4),'/',SUBSTR(payment_date, 4, 2),'/',SUBSTR(payment_date, 1, 2),' 00:00:00')) as payment_date_clean,
           FLOAT(REGEXP_REPLACE(transaction, ',', '')) as transaction_clean,
           FLOAT(REGEXP_REPLACE(invoice_amount, ',', '')) as invoice_amount_clean,
           supplier,
           description
        FROM [pete_test_dataset.pete_test_table]
    '''

    target_job_data = {'projectId': PROJECT_NUMBER,
                        'configuration': {
                            "query": {
                                "destinationTable": {
                                    "projectId": PROJECT_NUMBER,
                                    "tableId": TARGET_TABLE_ID,
                                    "datasetId": DATASET_ID,
                                },
                                "query": munging_query,
                                "createDisposition": "CREATE_IF_NEEDED",
                                "writeDisposition": "WRITE_APPEND",
                                }
                            }
                        }
    load_table(bigquery_service, PROJECT_NUMBER, target_job_data)

    # Now check we can query the data
    query_data = {
        'query': 'SELECT * FROM [pete_test_dataset.pete_test_target] LIMIT 20;'}
    query_request = bigquery_service.jobs()

    # Make a call to the BigQuery API
    query_response = query_request.query(projectId=PROJECT_NUMBER,
                                         body=query_data).execute()

    print 'Query Results:'
    for row in query_response['rows']:
        result_row = []
        for field in row['f']:
            result_row.append(field['v'])
        print ('\t').join(result_row)


# Loads the table from Google Cloud Storage and prints the table.
def load_table(service, projectId, jobData):
    try:
        jobCollection = service.jobs()
        insertResponse = jobCollection.insert(projectId=projectId,
                                              body=jobData).execute()

        print('Job Ref: ' + insertResponse['jobReference']['jobId'])

        # Ping for status until it is done, with a short pause between calls.
        import time
        while True:
            job = jobCollection.get(projectId=projectId,
                                    jobId=insertResponse['jobReference']['jobId']).execute()
            if 'DONE' == job['status']['state']:
                print 'Done Loading!'
                return

            print 'Waiting for loading to complete...'
            time.sleep(10)

        if 'errorResult' in job['status']:
            print 'Error loading table: ', pprint.pprint(job)
            return

    except HttpError as err:
        print 'Error in loadTable: ', pprint.pprint(err.resp)


def delete_table(bigquery_service, PROJECT_NUMBER, DATASET_ID, TABLE_ID):
    try:
        table_resource = bigquery_service.tables().get(projectId=PROJECT_NUMBER, datasetId=DATASET_ID, tableId=TABLE_ID).execute()
        if table_resource is not None:
            bigquery_service.tables().delete(projectId=PROJECT_NUMBER, datasetId=DATASET_ID, tableId=TABLE_ID).execute()
    except HttpError:
        print('Table not found, no need to delete ...')

if __name__ == "__main__":
    main()
