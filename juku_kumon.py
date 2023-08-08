import csv

import requests.exceptions
from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class JukuKumon(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited_links = list()
    self.PROXIES = {"REDACTED URL" 'REDACTED URL}

    start = time.time()
    with open('jp-cities-coordinates/points_5_10km_terbaru.csv', 'r', encoding='utf8') as f:
      reader = csv.DictReader(f)
      for coord in reader:
        if int(coord['index']) < 3737: continue
        print('--', coord['index'], coord['ADM1_EN'])
        data = {
          'age': 'noSelect',
          'open': 'noSelect',
          'searchAddress': '',
          'online': 'noSelect',
          'cx': float(coord['lon'])*3600,
          'cy': float(coord['lat'])*3600,
          'xmin': (float(coord['lon'])*3600)-2000,
          'xmax': (float(coord['lon'])*3600)+2000,
          'ymin': (float(coord['lat'])*3600)-1800,
          'ymax': (float(coord['lat'])*3600)+1800,
          'scaleId': '5',
          'isscale': '0',
          'code': '',
          'search_zip': '',
        }
        self.get_page(data)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    x = pd.DataFrame(self.content)

    # CLEANING 1: PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
    x['url_tenant'] = None

    try:
      self.df_clean = self.clean_data(x)
    except:
      raise
    if True:
      if from_main:
        self.df_clean.to_csv('D:/dags/csv/_juku_kumon2.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_juku_kumon2.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_juku_kumon2.csv', index=False)

  def get_page(self, data):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip(), 'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest'}
    try:
      store_lists = self.session.post("REDACTED URL"
                                    headers=headers, data=data).json()
    except requests.exceptions.JSONDecodeError:
      print(data)
      time.sleep(3)
      store_lists = self.session.post("REDACTED URL"
                                      headers=headers, data=data).json()
    except:
      print(data)
      raise

    for store in store_lists['classroomList']:
      if store['cid'] in self.visited_links: continue
      self.visited_links.append(store['cid'])
      _store = dict()

      _store['url_store'] = f"REDACTED URL"

      _store['store_name'] = store['rname']
_store['address'] = store['yubno'] +' '+ store['addr']

      _store['lat'], _store['lon'] = '',''

      _store['tel_no'] = store['ktelno']

      _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
    print(len(self.content))
    self.save_data()


if __name__ == '__main__':
  JukuKumon(True)
