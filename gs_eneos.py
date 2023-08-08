import json

import csv

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


class GsEneos(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_gs_eneos.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited = list()
    self.PROXIES = {"REDACTED URL"
                    'REDACTED URL}
    start = time.time()
    with open('jp-cities-coordinates/jp_coord.csv', 'r', encoding='utf8') as r:
      reader = csv.DictReader(r)
      count = 0
      for x in reader:
        count += 1
        print(count, x['city'])
        url = f"REDACTED URL"
        self.get_page(url)
        time.sleep(1)
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
    finally:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    while True:
      try:
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        break
      except:
        time.sleep(5)

    store_lists = re.findall(r'pointData\.unshift([^;]+);', page.content.decode('utf8'))
    for store in store_lists:
      store = store[1:-1].replace('\n', '').replace('\t', '').replace('\r', '')\
      .replace("'+  '", "").replace("'+ '", "").replace('"', '\\"').replace("'", '"')\
        .replace('{', '{"').replace(',', ',"').replace(':', '":').replace('font-size"', 'font-size')
      store = json.loads(store)
      data = self.get_data(store)
      if data:
        self.content.append(data)
    print(len(self.content))
    self.save_data()


  def get_data(self, json_store):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    details = BeautifulSoup(json_store[' text'], 'html.parser')
    if details.find('a')['href'] in self.visited: return
    self.visited.append(details.find('a')['href'])
    _store = dict()

    _store['url_store'] = "REDACTED URL"+ details.find('a')['href']

    _store['store_name'] = details.find('b').text + details.find('b').find_next_sibling('div').text
_store['address'] = details.find_all('div')[-2].text

    # maps_link = "REDACTED URL"+ 
_store['address'] + 
    # maps = self.get_map_data(maps_link)
    # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps)
    # print(coords)
    # if coords:
    #   _store['lat'] = coords[0].split(',')[0].replace('@', '')
    #   _store['lon'] = coords[0].split(',')[1]
    # else:
    _store['lat'] = ''
    _store['lon'] = ''

    _store['tel_no'] = details.find_all('div')[-1].text

    _store['open_hours'] = ''

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page.html.html


if __name__ == '__main__':
  GsEneos(True)
