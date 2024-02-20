# Big C: Special Assignment
This project aims to create a data pipeline for comparing prices of the top 100 best-selling smartphones at Big C Online, using data from the website https://www.priceza.com/.

## Description
- Programming languages: Python, SQL
- Tools/Technologies: Pandas, Docker, BeautifulSoup, Prefect Flow, PostgreSQL

## Data Pipeline Flow Explanation
1. Perform web scraping to extract data on the top 100 best-selling smartphones.
2. Clean the data obtained from Big C.
3. Search for 'phone_brand', 'phone_model', and 'phone_size' on the website 'https://www.priceza.com/' to find the path containing price comparison data.
4. Perform web scraping on the obtained paths.
5. Merge the data from Big C with Priceza.
6. Store the combined data in the database.

## Implement application
1. Run - > docker-compose build
2. Run - > docker-compose up -d