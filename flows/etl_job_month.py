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
import requests
import urllib.parse
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
import numpy as np
import time
import re
load_dotenv()


# read info from environment variables
postgres_info = {
    'username': os.environ['postgres-username'],
    'password': os.environ['postgres-password'],
    'host': os.environ['postgres-host'],
    'port': os.environ['postgres-port'],
    'database': os.environ['database']
}

# Set the timezone to Thailand timezone
thailand_timezone = pytz.timezone('Asia/Bangkok')
current_utc_datetime = datetime.utcnow()
current_thailand_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(thailand_timezone)

# All Task
@task(retries=3)
def top_100_title_smart_phone_best_sell(url_bigc):
    '''
    Get The top 100 best-sell group smart phone
    '''
    # Send a GET request to the specified URL
    response = requests.get(url_bigc)

    # Initialize lists to store titles and prices
    list_title_name = []
    list_price = []

    # Check if the request was successful
    if response.status_code == 200:
        # Extract the HTML content from the response
        html = response.text
        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        # Find all div elements with class containing 'productCard_title'
        title = soup.find_all('div', class_=lambda value: value and 'productCard_title' in value)
        # Find all div elements with class containing 'productCard_price'
        price = soup.find_all('div', class_=lambda value: value and 'productCard_price' in value)

        # Extract title information from the selected divs
        for div in title:
            # Extract and strip the text from the div
            title_name = div.text.strip()
            # Append the title to the list
            list_title_name.append(title_name)

        # Extract price information from the selected divs
        for div in price:
            # Extract and strip the text from the div
            price_product = div.text.strip()
            # Append the price to the list
            list_price.append(price_product)

        # Print the list of titles and return it along with the list of prices
        print("***Top 100 title smart phone best sell***", list_title_name)
        return list_title_name, list_price

    else:
        # If the request was not successful, raise an exception
        raise print("Failed to retrieve the webpage", "Status code:", response.status_code)


@task
def clean_data(list_title_name, list_price):
    # Function to extract phone model
    def extract_model(input_str):
        match = re.search(r'\s(.*?)\s(?:สี|ขนาด)', input_str)
        if match:
            output = match.group(1).replace('รุ่น ', '').strip()
            # Exclude 'GB' and content within parentheses
            name_type = ' '.join([i for i in output.split() if 'GB' not in i and '(' not in i])
            # Clean specific words
            for word in ['iPhone', 'เรดมี่', 'สมาร์โฟน', '2022', 'DTAC', 'สมารท์โฟน']:
                name_type = name_type.replace(word, '')
            name_type = name_type.replace('ไอโฟน', 'iPhone')
            name_type = name_type.replace('พลัส', 'Plus')
            # Remove extra spaces and return
            return ' '.join(name_type.split())
        else:
            if input_str == 'แอปเปิ้ล เพนซิล':
                input_str = input_str.replace('แอปเปิ้ล เพนซิล', 'Apple Pencil')
            return input_str

    # Function to extract phone size
    def extract_size(input_str):
        match = re.search(r'ขนาด\s+(.*?)\s*((GB|TB|สี))', input_str)
        if match:
            size = match.group(1)
            unit = match.group(2)
            if unit == 'สี':
                if len(unit) > 1:
                    unit = "GB"
                else:
                    unit = "TB"

            output = f'{size} {unit}'
            # Remove extra spaces and return
            return output.strip()
        else:
            match = re.search(r'\s(?!.*\s.*GB)(.*?GB)', input_str)
            if match:
                output = match.group(1)
                # Remove extra spaces and return
                if ' GB' not in output:
                    output = output.replace('GB', ' GB')
                return output.strip()
            return None

    # Function to extract color
    def extract_color(input_str):
        match = re.search(r'\s+สี(.*?)(?=ขนาด|$)', input_str)
        if match:
            output = match.group(1)
            return output.strip()
        else:
            return None

    # Function to map phone brands
    def mapping_brand(df):
        df_clean = df.copy()
        # Define the mapping dictionary
        mapping = {
            'แอปเปิ้ล': 'Apple',
            'วีโว่': 'Vivo',
            'ออปโป้': 'Oppo',
            'ซัมซุง': 'Samsung',
            'เรียลมี': 'Realme',
            'อินฟินิกซ์': 'Infinix',
            'เสี่ยวหมี่': 'Xiaomi'
        }
        # Replace values based on the mapping dictionary
        for pattern, code in mapping.items():
            df_clean['phone_brand'] = df_clean['phone_brand'].replace(to_replace=pattern, value=code, regex=True)

        # Handle missing values
        df_clean.loc[~df_clean['phone_brand'].isin(list(mapping.values())), 'phone_brand'] = 'Unknown'

        # Print the count of each brand
        print('*'*25)
        print(df_clean['phone_brand'].value_counts())

        return df_clean

    # Set the timezone to Thailand timezone
    thailand_timezone = pytz.timezone('Asia/Bangkok')
    current_utc_datetime = datetime.utcnow()
    current_thailand_datetime = current_utc_datetime.replace(tzinfo=pytz.utc).astimezone(thailand_timezone)
    weekly_path = datetime.strftime(current_thailand_datetime.today(), "%Y-%m-%d")

    # Clean the titles
    title = list_title_name.copy()
    title = [t.replace('สมาร์ทโฟน', '') for t in title]
    title = [t.replace('\u200b', '') for t in title]
    name = [t.split()[0] for t in title]  # Extract first word as name
    type_name = []
    size_memo = []
    title_list = []

    # Extract model, size, and type from each title
    for item in title:
        model = extract_model(item)
        size = extract_size(item)
        type_name.append(model)
        size_memo.append(size)
        title_list.append(item.strip())

    # Create dictionary for DataFrame
    data = dict()
    data['phone_brand'] = name
    data['phone_model'] = type_name
    data['phone_size'] = size_memo
    data['title'] = title_list
    data['price'] = [float(p.replace('฿', '').replace('/ เครื่อง', '').\
                              replace(',', '').replace('/ ชิ้น', '')) for p in list_price]

    # Create DataFrame
    df = pd.DataFrame(data)
    df['snapshot'] = current_thailand_datetime

    # Map phone brands
    df = mapping_brand(df)

    # Clean phone size column
    df['phone_size'] = df['phone_size'].str.replace(r'(.*?)/', '', regex=True)

    # Add additional columns
    df['web_shop'] = 'BigC'
    df['name_store'] = None
    df['rating_star'] = None
    df['path_source'] = None
    df['path_search'] = 'https://www.bigc.co.th/category/smartphone?limit=100&sort=bestsell'

    return  df


@task
def scrape_source_data(df_product):
    """
    This function scrapes data from https://www.priceza.com/
    """
    list_row = []  # List to store rows of scraped data
    url_target = 'https://www.priceza.com'  # Base URL for scraping

    # Loop through each row in the input DataFrame
    for row in df_product.values:
        list_data = []
        row = list(row)
        list_check = row.copy()
        list_path = row.copy()

        # Construct the URL path based on product information
        if row[2] is not None:
            list_path[2] = list_path[2].replace(' ', '')
            list_path = [d.lower().replace(' ', '-') for d in list_path]
            path = '-'.join(list_path)
            full_url = f"{url_target}/s/ราคา/{path}"
        else:  # If phone size is not available
            path = list_path[1].replace(' ', '-').lower()
            full_url = f"{url_target}/s/ราคา/{path}"

        time.sleep(15)
        response = requests.get(full_url)

        if response.status_code == 200:
            html = response.text  # Get the HTML content of the response
            soup = BeautifulSoup(html, 'html.parser')  # Parse the HTML using BeautifulSoup
            list_card = soup.find_all('div', class_=lambda value: value and 'pz-pdb-item pd-load-more-1' in value)  # Find all relevant card elements in the HTML

            # Loop through each card element
            for card in list_card:
                list_item = card.find_all('span', class_=lambda value: value and 'pz-pdb-price pd-group' in value)  # Find price elements within the card
                if len(list_item) > 0:
                    check = 0
                    title = card.find('div', class_=lambda value: value and 'pz-pdb_name pdbThumbnailName' in value).text.strip()
                    path = card.find('a').get('href').strip()
                    check_title = title.lower().replace(' ', '')  # Convert the title to lowercase and remove spaces

                    if None in list_check:
                        list_check.remove(None)  #

                    # Check if each element in the original row is present in the title of the product
                    for d in list_check:
                        if d.replace(' ', '').replace('-', '').lower() not in check_title:
                            break  # Break the loop

                    # Append the title, path, and full URL to the list of scraped data
                    list_data.append([title, path, full_url])

            if len(list_data) > 0:  # If scraped data is found
                # Append the original row along with the scraped data to the list of rows
                list_row.append(row + list_data[0])
            else:  # If no scraped data is found
                list_row.append(row + [None, None, full_url])  # Append the original row along with None values for scraped data and the full URL to the list of rows
        else:  # If the request is unsuccessful
            print('Not found', full_url)  # Print a message indicating that the URL was not found

    # Create a DataFrame from the list of rows with scraped data
    df_source = pd.DataFrame(list_row, columns=['phone_brand', 'phone_model', 'phone_size', 'phone_name', 'path_source', 'path_search'])
    return df_source  # Return the DataFrame containing scraped data


@task
def scrape_phone_price_analysis_data(df_source):
    # Extracting the old column names
    old_col = list(df_source.columns)
    product = []  # List to store scraped product data
    url_target = 'https://www.priceza.com'

    # Loop through each row in the input DataFrame
    for row in df_source.values:
        source = row[4] 
        path = f'{url_target}{source}'
        time.sleep(15)  # Pause execution for 15 seconds to avoid hitting the server too frequently
        response = requests.get(path)
        print('path', path)

        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            list_shop = soup.find_all('div', class_='pg-merchant-product')

            # Loop through each product element
            for shop in list_shop:
                img_tag = shop.find('img', class_='lazy')

                if img_tag and 'alt' in img_tag.attrs:
                    web_shop = img_tag['alt']

                list_product = shop.find_all('div', class_='pg-merchant-product__body-row')

                # Loop through each product detail element
                for p in list_product:
                    title = p.find('span', class_='pg-merchant-product__name')
                    name_store = p.find('div', class_='pg-merchant-product__store--name')
                    price = p.find('span', class_='pg-child-price')
                    rating_star = p.find('div', class_='pg-merchant-product__store--rating')
                    location = p.find('div', class_='pg-merchant-product__store--location')

                    # Extract product details if available
                    if title:
                        title = title.text.strip()
                    else:
                        title = None

                    if name_store and len(name_store) > 0:
                        name_store = name_store.text.strip()
                    else:
                        name_store = None

                    if price:
                        price = price.text.strip().replace('฿', '').replace(',', '')
                    else:
                        price = None

                    if rating_star:
                        rating_star = rating_star.text.strip().replace(' ', '') 
                        pattern = r'(\d+\.\d+)'
                        match = re.search(pattern, rating_star)

                        if match:
                            rating_star = match.group(1).strip()
                    else:
                        rating_star = None

                    if location:
                        location = location.find('span')

                        if location:
                            location = location.text.strip()
                        else:
                            location = None
                    else:
                        location = None

                    # Append the product details along with the original row data to the list of products
                    product.append(list(row) + [web_shop, title, name_store, rating_star, location, price])

        else:  # If the request is unsuccessful
            print('notfound', path)  # Print a message indicating that the URL was not found

    # Create a DataFrame from the list of products
    df_sor = pd.DataFrame(product, columns=(old_col + ['web_shop', 'title', 'name_store', 'rating_star', 'location', 'price']))

    # Replace empty strings in 'name_store' column with None
    df_sor.loc[df_sor['name_store'] == '', 'name_store'] = None

    # Add a 'snapshot' column with the current Thailand datetime
    df_sor['snapshot'] = current_thailand_datetime

    return df_sor

@task
def load_postgres(customer, table_name):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['database'] # each user has their own database in their username
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)
    print(f"Writing to database {postgres_info['username']}.{table_name} with {customer.shape[0]} records")
    customer.to_sql(table_name, engine, if_exists='append', index=False)
    print("Write successfully!")
    



@flow(log_prints=True)
def pipeline():
    '''
    Flow ETL month pipeline for phone price analysis

    This function orchestrates the ETL (Extract, Transform, Load) pipeline
    for the phone price analysis.
    '''
    # Print current date and time in Thailand
    print("Current Date and Time in Thailand:", current_thailand_datetime)
    # Print current date in Thailand
    print("Current Date in Thailand:", current_thailand_datetime.today().date())
    url_target = ''
    try:
        # Define the URL for fetching top 100 smartphone titles
        url_bigc = 'https://www.bigc.co.th/category/smartphone?limit=100&sort=bestsell'
        # Extract top 100 smartphone titles and prices
        list_title_name, list_price = top_100_title_smart_phone_best_sell(url_bigc)
        # Clean the extracted data
        df_top = clean_data(list_title_name, list_price)

        # Extract relevant columns for scraping source data
        df_product = df_top.loc[:, ['phone_brand', 'phone_model', 'phone_size']]
        # Remove duplicate rows
        df_product.drop_duplicates(inplace=True)

        # Scrape source data
        df_source = scrape_source_data(df_product)
        df_source.dropna(subset=['path_source'], inplace=True)
        
        # Scrape phone price analysis data
        df_price_analysis = scrape_phone_price_analysis_data(df_source)

        # Define the order of columns for the final DataFrame
        order_col = ['phone_brand', 'phone_model', 'phone_size', 'web_shop',
                     'title', 'name_store', 'rating_star', 'path_source',
                     'path_search', 'price', 'snapshot']
        # Select and reorder columns in the phone price analysis DataFrame
        df_price_analysis = df_price_analysis.loc[:, order_col]
        df_top = df_top.loc[:, order_col]

        # Concatenate the top 100 data and the scraped analysis data
        result = pd.concat([df_top, df_price_analysis], ignore_index=True)
        # load to DB
        if result.shape[0] != 0:
            load_postgres(result, 'phone_price_analysis')

    except Exception as e:
        # Raise an exception if the pipeline fails to execute
        raise Exception("Pipeline terminated because system could not extraction data: " + str(e))


if __name__ == '__main__':
    pipeline.serve(name="phone_price_analysis_month_pipeline", schedule=(CronSchedule(cron="0 23 1 * *", timezone="Asia/Bangkok")))
