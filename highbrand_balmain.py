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


class HighbrandBalmain(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_highbrand_balmain'.replace('/', '_')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
    self.get_page(url)
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
    headers = {'user-agent': UserAgent().random.strip()}
    store_lists = self.session.get(url, headers=headers).json()

    for store in store_lists:
      if store['countryCode'] != 'JP': continue
      print(store['name'])
      _store = dict()

      _store['url_store'] = "REDACTED URL"

      _store['store_name'] = store['name']
_store['address'] = store['address1'] +' '+ store['postalCode'] if store['postalCode'] else store['address1']

      maps_link = "REDACTED URL"+ 
_store['address']

      _store['lat'], _store['lon'] = '', ''



      try: _store['tel_no'] = store['phone']
      except KeyError: _store['tel_no'] = ''

      open_hours = ' '.join([f"{x['day']} {x['start']}~{x['end']}" for x in store['storeHoursSchedule']])
      _store['open_hours'] = open_hours

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      print(_store)
      self.content.append(_store)
    self.save_data()
    print(len(self.content))

if __name__ == '__main__':
  HighbrandBalmain(True)
