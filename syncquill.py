from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import pickle
import os
import argparse

TOKEN_PICKLE_PATH = "token.pickle"
CREDS_PATH = "credentials.json"

def authenticate():
    SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets']
    SCOPES_BIGQUERY = ['https://www.googleapis.com/auth/bigquery']

    creds = None
    if os.path.exists(TOKEN_PICKLE_PATH):
        with open(TOKEN_PICKLE_PATH, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDS_PATH, SCOPES_SHEETS + SCOPES_BIGQUERY)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_PICKLE_PATH, 'wb') as token:
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

def get_empty_cell_name(row, cols):
    for idx, ele in enumerate(row): 
        if ele == '': 
            return cols[idx]
    return ''

def google_sheets_data(service, spreadsheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    cols = values[0]
    values.pop(0)
    slno_index = cols.index('slno')
    for i, row in enumerate(values):
        if len(row) <= slno_index or not row[slno_index]:
            print(f"Error: Missing 'slno' in row {i + 2}. Please ensure every row has a 'slno'.")
            exit(0)
        
        if len(row) < len(cols) or any(not cell for cell in row):
            print(f"Error: Row {i + 2} contains empty values in coloumn {get_empty_cell_name(row, cols)}. Please ensure every cell is filled.")
            exit(0)

    return cols, values

def update_bigquery(client, dataset_id, table_id, changes, schema):
    for change in changes:
        if(change['type'] == 'diff'):
            query = f"""
                UPDATE `{dataset_id}.{table_id}`
                SET {change['changes']['col']} = '{change['changes']['new_value']}'
                WHERE slno = {change['changes']['slno']}
            """
            try:
                client.query(query).result()
            except Exception as e: 
                print(f"ERROR: Failed to update the table: {str(e)}")
        elif change['type'] == 'extra_row':
            converted_rows = [dict(zip(schema, row)) for row in change['rows']]
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            table_ref = client.dataset(dataset_id).table(table_id)
            job = client.load_table_from_json(converted_rows, table_ref, job_config=job_config)
            try: 
                job.result()
            except Exception as e: 
                print(f"ERROR: Failed to insert to the table: {str(e)}")

        elif change['type'] == 'del_row':
            for row in change['rows']:
                query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE slno = {row[0]}"
                try:
                    query_job = client.query(query).result()
                except Exception as e: 
                    print(f"ERROR: Failed to delete a row from the table: {str(e)}")

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

def parse_arguments():
    parser = argparse.ArgumentParser(description="Your tool description here")

    parser.add_argument("--spreadsheet_id", help="ID of the spreadsheet.")
    parser.add_argument("--sheet_name", help="Name of the sheet. Can include the range as well.")
    parser.add_argument("--dataset_id", help="ID of the bigquery dataset.")
    parser.add_argument("--table_id", help="ID of the bigquery table inside the given dataset.")

    return parser.parse_args()

def load_env_or_args(arg_value, env_var_name):
    if arg_value is None:
        return os.environ[f"{env_var_name}"]
    return arg_value


def pretty_print(changes, cols):
    def print_table(rows):
        if not rows:
            return
        
        # Calculate column widths
        col_widths = [max(len(str(item)) for item in col) for col in zip(*rows)]
        
        # Print horizontal separator line
        separator = '+' + '+'.join('-' * (width + 2) for width in col_widths) + '+'
        
        print(separator)
        
        # Print header
        header = '| ' + ' | '.join(f"{col:<{col_widths[i]}}" for i, col in enumerate(rows[0])) + ' |'
        print(header)
        print(separator)
        
        # Print rows
        for row in rows[1:]:
            row_str = '| ' + ' | '.join(f"{col:<{col_widths[i]}}" for i, col in enumerate(row)) + ' |'
            print(row_str)
        print(separator)

    all_changes = [["Status"] + cols]
    for change in changes:
        if change['type'] == 'diff':
            old_row = ["Old"] + [change['changes']['slno']] + [change['changes']['old_value'] if cols[i] == change['changes']['col'] else "" for i in range(1, len(cols))]
            new_row = ["New"] + [change['changes']['slno']] + [change['changes']['new_value'] if cols[i] == change['changes']['col'] else "" for i in range(1, len(cols))]
            all_changes.append(old_row)
            all_changes.append(new_row)
            all_changes.append([""] * (len(cols) + 1))  # Add a blank row for spacing
        elif change['type'] == 'extra_row':
            print("\nAdded new rows:")
            rows = [["Status"] + cols]
            for row in change['rows']:
                rows.append(["New"] + row)
            print_table(rows)
        elif change['type'] == 'del_row':
            print("\nDeleted rows:")
            rows = [["Status"] + cols]
            for row in change['rows']:
                rows.append(["Old"] + row)
            print_table(rows)
    
    # Remove the last blank row if exists
    if all_changes and all_changes[-1] == [""] * (len(cols) + 1):
        all_changes.pop()

    if all_changes:
        print("\nRow changes:")
        print_table(all_changes)

def main():
    sheets_service, bq_client = authenticate()
    load_dotenv()
    args = parse_arguments()
    spreadsheet_id = load_env_or_args(args.spreadsheet_id, "spreadsheet_id")
    sheet_name = load_env_or_args(args.sheet_name, "sheet_name")
    dataset_id = load_env_or_args(args.dataset_id, "dataset_id")
    table_id = load_env_or_args(args.table_id, "table_id")

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
