from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
import json
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class BankShizuokabank(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_bank_shizuokabank.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    strtime = str(time.time()).replace('.', '')[:13]
    url = f"REDACTED URL"
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
    headers = {'user-agent': UserAgent().random}
    pref_page = self.session.get(url, headers=headers)
    prefs = re.sub('.+result\((.+)\);.+', '\g<1>', pref_page.html.html)
    prefs = json.loads(prefs)
    for pref in prefs['mbml']['CountList']['Count']:
      headers = {'user-agent': UserAgent().random}
      strtime = str(time.time()).replace('.', '')[:13]
      pref_link = f"REDACTED URL"\
                  f'kencode={pref["count_id"]}&callback=cityresult&noCacheIE={strtime}'
      cities_page = self.session.get(pref_link, headers=headers)
      cities = re.sub('.+result\((.+)\);.+', '\g<1>', cities_page.html.html)
      cities = json.loads(cities)
      if isinstance(cities['mbml']['CountList']['Count'], dict):
        cities['mbml']['CountList']['Count'] = [cities['mbml']['CountList']['Count']]
      for city in cities['mbml']['CountList']['Count']:
        headers = {'user-agent': UserAgent().random}
        strtime = str(time.time()).replace('.', '')[:13]
        city_link = f"REDACTED URL"\
                    f"citycode={city['count_id']}&vo=mbml&json=1&pm=100&start=1&callback=storeresult&srt=disp_order&noCacheIE={strtime}"
        store_page = self.session.get(city_link, headers=headers)
        stores = re.sub('.+result\((.+)\);.+', '\g<1>', store_page.html.html)
        store_lists = json.loads(stores)
        if isinstance(store_lists['mbml']['PoiList']['Poi'], dict):
          store_lists['mbml']['PoiList']['Poi'] = [store_lists['mbml']['PoiList']['Poi']]
        for store in store_lists['mbml']['PoiList']['Poi']:
          if 'B' in store['id']: continue
          _store = dict()

          _store['url_store'] = f"REDACTED URL"

          _store['store_name'] = store['name']
_store['address'] = store['zip_code'] + store['full_address']

          _store['lat'] = ''

          _store['lon'] = ''

          _store['tel_no'] = store['tel'].replace('＜BR/＞', ' ')

          try: office_hour = store['open_hour_week'].replace('＜BR/＞', ' ')
          except: office_hour = ''
          try:
            atm_hour = store['atm_hour_week'].replace('＜BR/＞', ' ')
            _store['store_name'] = 'ATM' + _store['store_name']
          except: atm_hour = ''
          _store['open_hours'] = office_hour + ' ATM ' + atm_hour if atm_hour != '' else office_hour

          _store['gla'] = ''

          _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

          # print(_store)
          self.content.append(_store)
        self.save_data()
        print(len(self.content))


if __name__ == '__main__':
  BankShizuokabank(True)
