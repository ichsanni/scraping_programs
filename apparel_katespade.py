import csv
import json

from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass

class ApparelKatespade(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main
    self.visited_stores = list()
    self.file_name = '/apparel/katespade'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.PROXIES = {"REDACTED URL" 'REDACTED URL}
    self.start = time.time()
    with open('jp-cities-coordinates/prefektur.csv', 'r', encoding='utf8') as f:
      reader = csv.DictReader(f)
      for row in reader:
        ll = [float(row['lat']) + 0.5, float(row['lat']) + 0.5, float(row['lat']) - 0.5, float(row['lat']) - 0.5]
        ln = [float(row['lon']) + 0.5, float(row['lon']) - 0.5, float(row['lon']) + 0.5, float(row['lon']) - 0.5]
        for lat, lon in zip(ll, ln):
          print(row['nama'])
          self.url = f"REDACTED URL"
          self.get_page(self.url)
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
If no, call get_data(self.url) and visit individual pages.
"""
    headers = {'user-agent': UserAgent().random.strip(),'Accept': 'application/json','Referer': "REDACTED URL"}
    store_lists =self.session.get(url, headers=headers, ).json()    # proxies=self.PROXIES).json()
    time.sleep(1)
    content = list()
    for store in store_lists['response']['entities']:
      store = store['profile']
      if store['websiteUrl'] in self.visited_stores: continue
      self.visited_stores.append(store['websiteUrl'])
      _store = dict()
      _store['url_store'] = store['websiteUrl']
      _store['store_name'] = store['name']address = store['address']
      addr_subl = address['sublocality'] if address['sublocality'] else ''
      addr_line1 = address['line1'] if address['line1'] else ''
      addr_line2 = address['line2'] if address['line2'] else ''
      addr_line3 = address['line3'] if address['line3'] else ''
      
_store['address'] = address['postalCode'] + address['region'] + address['city'] + addr_subl + addr_line1 + addr_line2 + addr_line3
      try:
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

      except KeyError:
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

      _store['tel_no'] = store['mainPhone']['display']
      _store['open_hours'] = ''
      _store['gla'] = ''
      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
      self.content.append(_store)
    self.save_data()
    print(len(self.content))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  ApparelKatespade(True)

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
