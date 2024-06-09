#%% 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from pathlib import Path



from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from pendulum import datetime

#Functions for etl
def get_content(url_to_search):
    """
    Used for accessing a url and returns a response code
    resp = 200 means sucessfull connection
    """
    resp = requests.get(url_to_search)
    return resp

def get_objects(soup, selector):
    """
    Returns all the childs contained in the selector
    """
    return soup.select(selector)

def scrape_store_process(div_group):
    countries_info = {}
    for div in div_group:
        country_name = div.find('h3', class_="country-name").text.strip()
        country_capital = div.find('span', class_="country-capital").text.strip()
        country_population = div.find('span', class_="country-population").text.strip()
        country_area = div.find('span', class_="country-area").text.strip()        
        
        #Dict for storing the data
        countries_info[country_name] = {
            "Capital":country_capital,
            "Population":country_population,
            "Area (km2)":country_area
        }
    return countries_info


@dag(
    start_date=datetime(2024, 1, 1), 
    schedule="@daily",
    tags=["activity"],
    max_active_runs=3, 
    catchup=False
)
    
def ingestion_countries_data():
    @task
    def ingest_landing_data():
        # Functions outside the task
        web_data = get_content("https://www.scrapethissite.com/pages/simple/")
        soup = BeautifulSoup(web_data.text, 'html.parser')
        countries_divs = get_objects(soup, 'div[class="col-md-4 country"]')
        landing_dictionary = scrape_store_process(countries_divs)
        return landing_dictionary

    @task
    def convert_and_save(dicionario):
        # table = pd.DataFrame.from_dict(dicionario, orient='index')
        # return table.head(5)
        table = pa.table(dicionario)
        output_buffer = io.BytesIO()
        pq.write_table(table, output_buffer)
        parquet_data = output_buffer.getvalue()
        # Save the parquet to a local directory, for this, i will change the yaml setting volumes to add
        # Local Directories, atm this is not possible
    
    landing_data = ingest_landing_data()
    landing_data >> convert_and_save(landing_data)

ingestion_countries_data()