from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
scraped_table_attribs = ["Name", "MC_USD_Billion"]
final_table_attribs = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_path = 'code_log.txt'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_path,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
        

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=scraped_table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')[1:]
    for row in rows:
        col = row.find_all('td')
        data_dict = {"Name": col[1].text.replace('\n', ""),
                     "MC_USD_Billion": col[2].text.replace('\n', "")}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, scraped_table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
log_progress('Data transformation complete. Initiating Loading process')
log_progress('Data saved to CSV file')
log_progress('SQL Connection initiated')
log_progress('Data loaded to Database as a table, Executing queries')
log_progress('Process Complete')
log_progress('Server Connection closed')
