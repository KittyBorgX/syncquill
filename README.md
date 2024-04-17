# syncquill
Sync data between Google Sheets and Bigquery Database

### What is this? 
This is a tool to make update data into bigquery directly from google sheets. It's helpful for non-software clients who need to update the database (existing) without having to learn and use DML or SQL. Currently used interally for a project.

### Note (important)

This tool currently supports editing of an existing row in your google sheets but I plan to add the functionality of inserting rows very soon!


### Steps (More details with links coming soon but here's an overview)

1. Create a Google Cloud project if you haven't already
2. Enable the bigquery api and create a dataset
3. In your Google Sheets, make sure that you have the row 1 with your headings
4. In the "A" coloumn of your google sheets, add a "slno" field with the serial number of the entries in your sheets. That is, A1 should be "slno" and subsequent rows below should have a unique indentifier of the serial number (can just be 1, 2, 3)
5. Download the current Google Sheet as CSV (Linking via Google Sheets Link doesn't autodetect schema for some reason.)
6. Create a table in bigquery and upload the CSV file and make sure to enable the "auto detect schema" option.
7. Make an oauth client id in https://console.cloud.google.com/apis/credentials (https://support.google.com/cloud/answer/6158849?hl=en). Remember to make it as a "Desktop" token and not a web token.
8. Download the credentials (json) and save it in this project root as `credentials.json`
7. Once your table is created, you can edit the google sheet, run this tool (instructions below) and your bigquery table should be updated. 

### Building and setup

1. Clone this repository: 
```
git clone https://github.com/KittyBorgX/syncquill.git && cd syncquill
```

2. Copy `.env.example` to `.env` and fill out the details:
```
cp .env.example .env
```

3. Download the credentials (json) of the created OAuth Desktop Client (Step 7) and save it in this project root as `credentials.json`

(Optional) : Make a python virtual env 

4. Install the requirements: 
```
pip install -r requirements.txt
```

### Running the tool:

Once you've made a change in google sheets, run the tool to sync the data in bigquery using:
```
python syncquill.py
```

### Note on environment variables: 
- spreadsheet_id -> This should be the id as seen in the URL of the Google Sheets. 
- sheet_name -> This is the sheet name (usually Sheet1 for unchanged new sheets). Can include range as well in the syntax: `Sheet1!A1:M100` assuming Sheet1 is your sheet name
- dataset_id -> Dataset ID of your bigquery dataset
- table_id -> Table ID of your bigquery table