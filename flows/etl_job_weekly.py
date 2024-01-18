import boto3
from io import StringIO, BytesIO
import pandas as pd
import numpy as np
from prefect import task, flow, context
from prefect.client.schemas.schedules import CronSchedule
from datetime import datetime, timedelta
import pytz
import psycopg2
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

# # # fill your aws account name here
account_name = 'fongzxzxzx' 

# read info from environment variables
postgres_info = {
    'username': os.environ['postgres-username'],
    'password': os.environ['postgres-password'],
    'host': os.environ['postgres-host'],
    'port': os.environ['postgres-port'],
}

# Set the timezone to Thailand timezone
thailand_timezone = pytz.timezone('Asia/Bangkok')
current_utc_datetime = datetime.utcnow()
current_thailand_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(thailand_timezone)

# Connect AWS S3
aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
s3_client = session.client('s3')
# s3_client = boto3.client('s3')

# Extract data from source
bucket_name = 'fullstackdata2023'
folder_name = 'fongzxzxzx'
table_member = 'customer'

# Create source path 
weekly_path = datetime.strftime(current_thailand_datetime.today(), "%Y-%m-%d")

# weekly_path = "2023-12-31"
source_path = f"{folder_name}/{weekly_path}/lf_cafe_data.csv"

# All Task
@task(retries=3)
def check_s3_path(bucket_name, s3_path):
    """
    This function check path_s3 that is exists. 
    """
    print(f'>>>>> date run pipeline: {weekly_path}')
    result_check = False
    list_objects = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']
    for path_s3 in list_objects:
        if path_s3['Key'] == s3_path:
            print(f"The path 's3://{bucket_name}/{s3_path}' exists.")
            result_check = True
    return result_check


@task
def extract(bucket_name, source_path):
    '''
    Extract data from S3 bucket
    '''
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=source_path)
    # Read the CSV file content
    csv_string = StringIO(response['Body'].read().decode('utf-8'))
    # Use Pandas to read the CSV file
    df = pd.read_csv(csv_string)
    print(f"*** Extract df with {df.shape[0]} rows")
    return df

@task
def extract_postgres(table_name):
    '''
    Extract data from Postgres database
    '''
    database = postgres_info['username']  # each user has their own database in their username
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    print(f"Read table: {table_name} from Postgres yielding {df.shape[0]} records")
    return df

@task
def transform_cf_data(df_exaract_data):
    """
    transform data
    """
    # 1. Create bill data
    # 1.1 Copy data frame
    df_bill = df_exaract_data.copy()
    df_lf_p = pd.read_csv('/opt/prefect/flows/product_cat.csv')
    df_lf_p['category'] = df_lf_p['category'].fillna("NA")

    # 1.2 Create bill time stamp
    df_bill.loc[:,"bill_timestamp"] = df_bill['bill_date'] + ' ' + df_bill['bill_time'].str.replace('น.', '')
    df_bill["bill_timestamp"] = pd.to_datetime(df_bill["bill_timestamp"])
    df_bill.drop(['bill_time'], axis=1, inplace=True)

    # 1.3 Classifly sku_category
    df_bill['4_early_num_code'] = df_bill['sku_code'].astype(str).str[:4].astype(int)
    df_bill = df_bill.merge(df_lf_p, how='inner', left_on='4_early_num_code', right_on='code') # , suffixes=('', '_r')
    df_bill.drop(['4_early_num_code', 'code'], axis=1, inplace=True)
    df_bill.rename(columns = {'category': 'sku_category'}, inplace = True)

    # 1.4 Create snapshot
    df_bill['snapshot'] = current_thailand_datetime

    # 2. Create Customer data
    # 2.1 Copy data frame
    df_customer = df_bill.copy()

    # 2.3 Drop nan
    df_customer.dropna(subset=['member_id'], inplace=True)

    # 2.4 Sort DataFrame by 'customer_id' and 'order_date' in descending order
    df_customer.sort_values(['member_id', 'bill_date'], ascending=[True, False], inplace=True)

    # 2.5 Create a 'recency' column using groupby and transform to get the most recent date for each customer
    df_customer['recency'] = df_customer.groupby('member_id')['bill_date'].transform('max')

    # 2.6 Create Frequency
    df_customer["frequency_1w"] = df_customer.groupby("member_id")["bill_date"].transform("size")

    # 2.7 Create Monetary value
    df_customer['monetary_value'] = df_customer.groupby("member_id")["sales"].transform("sum")

    # 2.8 Drop duplicate and Sort DataFrame back to its original order
    df_customer = df_customer.drop_duplicates(subset='member_id', keep='last').reset_index()

    # 2.9 Select column
    df_customer = df_customer.loc[:,['member_id', 'recency', 'frequency_1w', 'monetary_value', 'snapshot']]

    # 3. Select column unuse
    df_bill.drop(['bill_date'], axis=1, inplace=True)

    return df_bill, df_customer

@task
def check_member(df, df_member):
    """
    Seclect data forand adding new member_id data 
    """
    df_exaract_data = df.copy()

    # Merge dataframes and filter for new members
    df_new_member = pd.DataFrame(
    pd.merge(
    df_exaract_data,
    df_member,
    on=['member_id'],
    how='outer',
    indicator=True,
    suffixes=['', 'r']
    ).query('_merge=="left_only"')
    .loc[:, 'member_id']
    ).reset_index(drop=True)
    df_new_member.dropna(inplace=True)

    # Add new member-related columns
    df_new_member['sex'] = np.nan
    df_new_member['year_of_birth'] = np.nan
    df_new_member['location'] = np.nan
    df_new_member['created_at'] = current_thailand_datetime
    df_new_member['updated_at'] = current_thailand_datetime
    return df_new_member
    
@task
def load_postgres(customer, table_name):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['username'] # each user has their own database in their username
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='append', index=False)
    print("Write successfully!")
    



@flow(log_prints=True)
def pipeline():
    '''
    Flow ETL daily pipeline 
    '''
    print("Current Date and Time in Thailand:", current_thailand_datetime)
    print("Current Date in Thailand:", current_thailand_datetime.today().date())
    
    if check_s3_path(bucket_name, source_path):
        # Eextract
        df_exaract_data = extract(bucket_name, source_path)
        # Transform
        df_bill, df_customer = transform_cf_data(df_exaract_data)
        print(f'>>>>> new bill data: {df_bill.shape[0]}')
        print(f'>>>>> new customer analysis data: {df_bill.shape[0]}')
        # Current member data
        df_member = extract_postgres('member_lf')
        df_new_member = check_member(df_bill, df_member)
        # Load
        if df_bill.shape[0] != 0:
            load_postgres(df_new_member, 'member_lf')
            load_postgres(df_bill, 'bill_lf')
            load_postgres(df_customer, 'member_analysis')
    else:
        raise Exception("Pipeline terminated because syterm not found data for exacting")

if __name__ == '__main__':
    pipeline.serve(name="weekly_pipeline", schedule=(CronSchedule(cron="15 23 * * 7", timezone="Asia/Bangkok")))
