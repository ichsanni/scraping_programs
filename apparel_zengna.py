import re

from fake_useragent import UserAgent
import datetime, time
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
from scraping.scraping import CleaningData
from google.cloud import storage


class ApparelZengna(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()

    url = "REDACTED URL"
    self.get_page(url)

    end = time.time()
    print("============ ", (end - start)/60, " minute(s) ============")
    # ===== MULAI KONEKSI KE BIGQUERY
    client = storage.Client()
    bucket = client.get_bucket('scrapingteam')
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
          
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_apparel_zengna.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_apparel_zengna.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
      raise

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))
  
  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    store_lists = page.json()

    for store in store_lists:
      _store = dict()

      address_in_url = store['ADDRESS'].strip().lower().replace(' ', '-').replace(',', '').replace('.', '').replace("'", '')
      _store['url_store'] = "REDACTED URL"+ store['STORE_ID']

      _store['store_name'] = store['NAME']
_store['address'] = store['ADDRESS'].replace(',', '') + ' ' + store['POSTAL_CODE']

      maps_link = "REDACTED URL"+ 
_store['address']

      _store['lat'], _store['lon'] = '', ''



      _store['tel_no'] = store['PHONE_NUMBER']

      _store['open_hours'] = store['OPENING_HOURS'].split(',')[0][2:]

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
    print(len(self.content))
        