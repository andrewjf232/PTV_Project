import botocore.exceptions
import requests
import json
import datetime
import time
import os 
from dotenv import load_dotenv
import boto3
import botocore

# load environment varibales into script
load_dotenv()

# configuration
API_KEY = os.getenv('TFNSW_API_TOKEN')
S3_BUCKET_NAME = 'ptv-pipeline-raw-data-ajf-24-4-25'

#paths within s3
STATION_REF_S3_KEY = 'raw/traffic_volume/station_reference/station_reference.csv'
HOURLY_PERM_S3_BASE_PATH = 'raw/traffic_volume/hourly_permanent/' # Base path for daily folders
# the s3 object is the path inside the bucket where the raw data is stored.

# API configuration
BASE_URL = 'https://api.transport.nsw.gov.au'
ENDPOINT = '/v1/traffic_volume'
FULL_URL = BASE_URL + ENDPOINT
OUTPUT_FORMAT = 'csv' 


# SQL File Names
STATION_REF_SQL_FILENAME = 'station_reference.sql'
HOURLY_PERM_SQL_FILENAME = 'hourly_permanent.sql'


# Backfill configuration
BACKFILL_DAYS = 365 # Number of past days to backfill
API_DELAY_SECONDS = 1 # Delay between API calls during backfill to be polite

def load_sql_query(filename):
    #loads a SQL query from a file.
    try:
        # Open the file in read mode ('r')
        # The 'with' statement ensures the file is closed automatically
        with open(filename, 'r') as f:
            # Read the entire content of the file into the variable
            sql_query = f.read().strip() # .strip() removes leading/trailing whitespace/newlines

        # Optional: Print the loaded query to confirm
        print(f"Loaded SQL query from {filename}:")
        #print(sql_query)
    except FileNotFoundError:
        print(f"Error: SQL file '{filename}' not found in the current directory.")
        print("Please make sure the file exists and the script has permission to read it.")
        # Assign a default value or exit if the file is essential
        sql_query = None # Or exit()
        exit() # Exit the script if the SQL file is missing
        # Check if query was loaded successfully before proceeding (if you didn't exit)
    if not sql_query:
        print("Exiting because SQL query could not be loaded.")
        exit()
    if not API_KEY:
        print(f"{API_KEY} not found in environment variables")
        exit()




#print(f"Saving to: {output_filename}")
# not saing locally anymore.


# --- Initialise s3 client --- 
# ensure AWS credentials are configured via environment variables
# shared credential file (~/.aws/credentials), or IAM role.
try:
    s3_client = boto3.client('s3')
    print("s3 client initialised successfully")
except Exception as e:
    print(f"Error initializing S3 client: {e}")



# --- Fetch Request From API ---
def fetch_data_from_Api(sql_query, api_key):
    headers = {
    'Authorization': f'apikey {API_KEY}',
    'Accept': f'application/{OUTPUT_FORMAT}'                      
    }

    params = {
        'q': sql_query,
        'format': OUTPUT_FORMAT
    }
    print(f"\nSending request to: {FULL_URL}")

    # --- Making API request --
    try:
        #requests.get() returns a requests.Response object. We must assign it to the response variable.
        response = requests.get(FULL_URL, headers=headers, params=params, timeout=60)

        # --- Check for SUCCESS (200 OK) ---
        if response.status_code == 200:
            print('\nRequest Successful!')
            return response.text   #returns csv data as string
        else:
            print(f"\nError status code with the code:{response.status_code}")
            print(f"DEBUG: Actual URL requested:{response.request.url}")
            print("Response Body:")
            print(response.text)
            return None # return None on failure.
    except requests.exceptions.RequestException as e:
        # Handle potential network errors (e.g., connection timeout)
        print(f"\nAn error occurred during the request: {e}")
        return None
    except requests.exceptions.Timeout:
        print("\nRequest timed out. Consider increasing the timeout value or checking network.")
        return None
    


# --- Upload Data to S3 --- 
def upload_to_s3(s3_client, bucket, key, data_body):
    """Uploads data string to a specific S3 key."""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data_body,
            ContentType='text/csv'
        )
        print(f'Successfully uploaded data to s3://{bucket}/{key}')
        return True
    except botocore.exceptions.ClientError as e:
        print(f'\nError uploading data to S3 (s3://{bucket}/{key}): {e}')
        return False
    except Exception as e:
        print(f'\nAn unexpected error occurred during S3 upload: {e}')
        return False
    

# --- Check s3 object exists ---     
def check_s3_object_exists(s3_client, bucket, key):
    """Checks if an object exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        print(f"Object s3://{bucket}/{key} already exists.")
        return True
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the object does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Object s3://{bucket}/{key} does not exist.")
            return False
        else:
            # Handle other errors (like permissions)
            print(f"Error checking S3 object s3://{bucket}/{key}: {e}")
            return None # Indicate an error occurred during check
    except Exception as e:
        print(f"Unexpected error checking S3 object: {e}")
        return None

# --- get s3 Key for Date ---
def get_s3_key_for_Date( base_path, target_date):
    # Hive partioning to construct key's path in s3
    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    day = target_date.strftime('%d')
    date_str = target_date.strftime('%Y-%m-%d')
    # Example: raw/traffic_volume/hourly_permanent/year=YYYY/month=MM/day=DD/hourly_permanent_YYYY-MM-DD.csv
    return os.path.join(
        base_path,
        f'year={year}',
        f'month={month}',
        f'day={day}',
        f'hourly_permanent_{date_str}.csv'
    ).replace("\\", "/") # Ensure forward slashes



# --- fetch and upload for one specific date ---
def fetch_and_upload_for_single_date( target_date, s3_client, api_key, sql_template):
   # Fetches and uploads hourly data for a specific date if it doesn't exist.
    date_str = target_date.strftime('%Y-%m-%d')
    print(f"\n--- Processing Hourly Data for Date: {date_str} ---")

    hourly_s3_key= get_s3_key_for_Date(HOURLY_PERM_S3_BASE_PATH, target_date)

    #check if data already exists
    exists = check_s3_object_exists(s3_client, S3_BUCKET_NAME, hourly_s3_key)
    if exists == True:
        print('s3 object exists')
        return True
    elif exists is None:
        print(f"Error checking for {date_str}. Skipping")

    # If date doesn't exist as key, need to fetch from API
    print(f"Data for {date_str} not found in S3. Fetching from API...")
    if not sql_template:
        print("Hourly SQL template is missing. Cannot fetch data.")
        return False
    
    # Construct the dynamic SQL query
    date_condition_sql = f"date = '{date_str}'"
    hourly_dynamic_sql = sql_template.replace('{date_condition}', date_condition_sql)

    #fetch from api
    hourly_data = fetch_data_from_Api(hourly_dynamic_sql, API_KEY)

    if hourly_data:
        #upload the fetched data to s3
        upload_success = upload_to_s3(s3_client, S3_BUCKET_NAME,hourly_s3_key, hourly_data)
        return upload_success
        print("Hourly Data for today is now uploading to s3")
    else:
        print(f"Failed to upload to s3 for {date_str}")
        return False
        # indicate failure.

def backfill_last_n_days(n_days, s3_client, API_KEY, sql_template):
    
    # for the last n_days, fetch data in a loop. When yesterday's data is collected, re run the
    #loop to fetch the prior day's. And then again so on.
    print(f"\n--- Starting Backfill Process for Last {n_days} Days ---")
    today = datetime.date.today()
    for i in range(n_days, 0, -1): # loop from end days until yesterday.
        target_date = today - datetime.timedelta(days=i)
        # Calculates the date that was exactly i days before the current date (today).
        success = fetch_and_upload_for_single_date(target_date, s3_client, API_KEY, sql_template)
        if not success:
            print(f"Failed to process data for {target_date}. Continuing backfill...")
        # Add a delay between requests
        print(f"Waiting {API_DELAY_SECONDS} second(s) before next request...")
        time.sleep(API_DELAY_SECONDS)
    print(f"--- Backfill Process Finished ---")





    
    

# --- Main Execution Logic---
def main(run_backfill = False): # added logic to control backfilling.
    print("Starting daily TfNSW data fetch and upload process...")

    # Validate API key
    if not API_KEY:
        print("API Key not found in environment variables")
        exit()

    # initialise s3 client
    try:
        s3_client = boto3.client('s3')
        print("s3 Client initialised successfully")
    except Exception as e:
        print(f"Unable to initialise client: {e}")
        exit()

    # --- 1. Process Station Reference Data (Static) ---
    print("\n--- Processing Station Reference Data ---")
    # Check if it already exists to avoid re-uploading
    exists = check_s3_object_exists(s3_client, S3_BUCKET_NAME, STATION_REF_S3_KEY)
    if exists == False:
        station_sql = load_sql_query(STATION_REF_SQL_FILENAME)
        #load the sql query into the station_sql variable.
        if station_sql:
            station_fetch_data = fetch_data_from_Api(station_sql, API_KEY)
            if station_fetch_data:
                upload_to_s3(s3_client, S3_BUCKET_NAME, STATION_REF_S3_KEY, station_fetch_data)
            else:
                print("Failed to fetch station reference data. Skipping upload.")
        else:
            print("Failed to load station reference SQL. Skipping.")
    else:
        print("Skipping station reference upload as it already exists.")


    # --- 2. Process Hourly Permanent Data (Daily) ---
    print("\n--- Processing Hourly Permanent Data ---")
    #calculate yesterday's date (common practice for daily batch jobs)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    print(f"Fetching data for date: {yesterday_str}") 

        # Load the base hourly SQL query
    hourly_sql = load_sql_query(HOURLY_PERM_SQL_FILENAME)
    if hourly_sql:
        # if hourly sql working then fetch
        try: 
            hourly_perm_data = fetch_data_from_Api(hourly_sql, API_KEY)
             # returns the response.text
            print(f"Reponse text successful and stored in {hourly_perm_data}")
            if hourly_perm_data:
                check_hourly_perm_exists = check_s3_object_exists(s3_client,S3_BUCKET_NAME,API_KEY)
                if check_s3_object_exists == False:
                    upload_hourly_perm_data = upload_to_s3(s3_client,S3_BUCKET_NAME,API_KEY, data_body=hourly_perm_data)
                else:
                    print(f"s3 Object already exists")
            else:
                print(f"getting response text from API was not successfull")
        except:
            print(f"API fetch failed")



