from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import bigquery
from dotenv import load_dotenv
import pickle
import os.path
import os


def authenticate():
    SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets']
    SCOPES_BIGQUERY = ['https://www.googleapis.com/auth/bigquery']

    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES_SHEETS + SCOPES_BIGQUERY)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    sheets_service = build('sheets', 'v4', credentials=creds)
    bq_client = bigquery.Client(credentials=creds)

    return sheets_service, bq_client

def bigquery_data(bq_client: bigquery.Client, dataset_id, table_id):
    try:
        query = f"""
        SELECT * FROM `{dataset_id}.{table_id}`
        """
        query_job = bq_client.query(query)
        results = query_job.result()

        my_data = [row.values() for row in results]
        sorted_dat = sorted(my_data, key=lambda x: x[0])
        new_dat = [list(entry) for entry in sorted_dat]

        return new_dat

    except Exception as e:
        print(f"Failed to retrieve data from BigQuery: {str(e)}")

def google_sheets_data(service, spreadsheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    cols = values[0]
    values.pop(0)
    return cols, values

def update_bigquery(client, dataset_id, table_id, changes, schema):
    for change in changes:
        if(change['type'] == 'diff'): 
            query = f"""
                UPDATE `{dataset_id}.{table_id}`
                SET {change['changes']['col']} = '{change['changes']['new_value']}'
                WHERE slno = {change['changes']['slno']}
            """
            print("Updated bigquery for change - ", change)
            client.query(query).result()
        elif change['type'] == 'extra_row':
            converted_rows = [dict(zip(schema, row)) for row in change['rows']]
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            table_ref = client.dataset(dataset_id).table(table_id)
            job = client.load_table_from_json(converted_rows, table_ref, job_config=job_config)
            job.result()
            print("Rows inserted successfully into BigQuery.")
        elif change['type'] == 'del_row':
            for row in change['rows']:
                query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE slno = {row[0]}"
                query_job = client.query(query).result()
                print(f"Row with serial number '{row[0]}' deleted successfully from BigQuery.")

def compare_data(list1, list2, cols):
    # Convert all elements of both the lists to strings since bigquery results store the serial numbers as integers
    # while google sheets stores all values as strings
    list1 = [[str(element) for element in sublist] for sublist in list1]
    list2 = [[str(element) for element in sublist] for sublist in list2]
    
    changes = []

    if len(list1) > len(list2):
        extra_rows = list1[len(list2):]
        changes.append({'type': 'del_row', 'rows': extra_rows})
        
    if len(list2) > len(list1):
        extra_rows = list2[len(list1):]
        changes.append({'type': 'extra_row', 'rows': extra_rows})
    
    for i in range(min(len(list1), len(list2))):
        for j in range(min(len(list1[i]), len(list2[i]))):
            if list1[i][j] != list2[i][j]:
                changes.append({'type': 'diff', 'changes': {'slno': list1[i][0], 'col': cols[j], 'old_value': list1[i][j], 'new_value': list2[i][j]}})
    return changes



def main():
    sheets_service, bq_client = authenticate()
    load_dotenv()
    spreadsheet_id = os.environ["spreadsheet_id"]
    sheet_name = os.environ["sheet_name"]
    dataset_id = os.environ["dataset_id"]
    table_id = os.environ["table_id"]

    cols, sheets_dat = google_sheets_data(sheets_service, spreadsheet_id, sheet_name)
    bigquery_dat = bigquery_data(bq_client, dataset_id, table_id)

    changes = compare_data(bigquery_dat, sheets_dat, cols)
    print("-------------- Changes ---------------")
    for element in changes:
        print(element)
    print("--------------------------------------")

    if changes:
        update_bigquery(bq_client, dataset_id, table_id, changes, cols)

if __name__ == '__main__':
    main()
