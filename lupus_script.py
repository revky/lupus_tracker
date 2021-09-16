import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
from datetime import datetime
import schedule
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

EXCEL_FILE = 'margonem_ranking_lupus.xlsx'
link = "https://www.margonem.pl/ladder/players,Lupus"

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)
req = session.get(link)

page_avalible = True if req.status_code == 200 else False
file_exists = True if EXCEL_FILE in os.listdir() else False

soup = BeautifulSoup(req.content, 'html5lib')

def transform_to_proper_format(col):
  return col.str.split().str[:-1].str.join("").str.findall(r"(\d+)(\w+)").str[0]

def transform_to_active_or_inactive(value):
  return 'Yes' if value <= 10 else 'No'

def transform_time_to_minutes(tpl):
  if str(tpl) == 'nan':
    return 0
  value = int(tpl[0])
  multiplier = tpl[1]
  multiplier_map = {'h':60, 'min':1,'dni': 1440}
  return value * multiplier_map.get(multiplier)

def return_scraped_dataframe():
  soup = BeautifulSoup(req.content, 'html5lib')
  total_pages = soup.find('div', attrs = {'class':'total-pages'}).text.strip()
  df = pd.DataFrame()

  for i in range(1,int(total_pages) + 1):
    df = df.append(pd.read_html(f'{link}?page={i}'))
  return df

def process_dataframe(dataframe):
  logged_with_date = f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}'
  dataframe.columns = ['id','player','level','class','ph',logged_with_date]
  dataframe.set_index('player', inplace = True)
  dataframe.drop('id', axis = 1, inplace= True)
  dataframe[logged_with_date] = transform_to_proper_format(dataframe[logged_with_date])
  dataframe[logged_with_date] = dataframe[logged_with_date].apply(transform_time_to_minutes).apply(transform_to_active_or_inactive)
  return dataframe, logged_with_date

def main():
  print('Start of script')

  curr_df = return_scraped_dataframe()
  curr_df, logged_with_date  = process_dataframe(curr_df)
  print('Dataframe processed')

  if page_avalible and not file_exists:
    curr_df.to_excel(EXCEL_FILE)
    print('New file created')

  elif page_avalible and file_exists:
    old_df = pd.read_excel(EXCEL_FILE)
    curr_df_selected_cols = curr_df[logged_with_date]
    old_df = old_df.merge(curr_df_selected_cols, on = 'player')
    old_df.to_excel(EXCEL_FILE)
    print('New data added, file updated.')

schedule.every(10).seconds.do(main)

while True:
  schedule.run_pending()
  time.sleep(1)