import csv
import random

from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests.exceptions import ChunkedEncodingError
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass

class ConveniLawson100(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main

    self.file_name = '/conveni/lawson100'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.PROXIES = {"REDACTED URL" 'REDACTED URL}
    self.visited_stores = list()
    self.start = time.time()
    try:
      with open('jp-cities-coordinates/jp_coord.csv', 'r', encoding='utf8') as f:
        self.index_city = 0
        reader = csv.DictReader(f)
        for city in reader:
          print('--', city['city'])
          self.url = f"REDACTED URL"
          self.get_page(self.url)
          time.sleep(1)
    except:
      raise
    if True:
      self.end = time.time()

    x = pd.DataFrame(self.content)
    if len(x) == 0:
      raise ValueError('Empty df')

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
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    """
Visit the link given from the gsheet,
see if there's data there.
If yes, scrape immediately.
If no, call get_data(url) and visit individual pages.
"""
    headers = {'user-agent': UserAgent().random.strip()}
    while True:
      try:
        page = self.session.get(url, headers=headers, )    # proxies=self.PROXIES)
        break
      except:
        time.sleep(7)

    store_lists = page.content.decode('euc-jp').split('=')[1].split('\\n')[1:]
    for store in store_lists:
      store = store.split('\\t')
      if store[0] not in self.visited_stores:
        self.visited_stores.append(store[0])
        _store = dict()
        _store['url_store'] = "REDACTED URL"
        try:
          _store['store_name'] = store[6]
        except IndexError:
          continue
_store['address'] = store[7]
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = store[8]
        _store['open_hours'] = '24時間営業' if str(store[10]) == '1' else str(store[11]) + '~' + str(store[12])
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        self.content.append(_store)
        self.save_data()
    print(len(self.visited_stores))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  ConveniLawson100(True)

class AsyncHTMLSessionFixed(AsyncHTMLSession):
  """
  pip3 install websockets==6.0 --force-reinstall
  """

  def __init__(self, **kwargs):
    super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
    self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])

  @property
  async def browser(self):
    if not hasattr(self, "_browser"):
      self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
                                             handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

    return self._browser
