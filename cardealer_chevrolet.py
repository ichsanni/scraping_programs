import datetime, csv, time
import re

from requests_html import HTMLSession
from fake_useragent import UserAgent
# IMPORT PENTING
import pandas as pd
from scraping.scraping import CleaningData
from google.cloud import storage


class CardealerChevrolet(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.visited_url = list()
    self.content = list()

    start = time.time()
    with open('jp-cities-coordinates/prefektur.csv', 'r', encoding='utf8') as f:
      reader = csv.DictReader(f)
      for row in reader:
        print('name:', row['nama'])
        url = f"REDACTED URL"
        self.get_page(url)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

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
      bucket.blob('/ichsan/_cardealer_chevrolet.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_cardealer_chevrolet.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
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
    headers = {'user-agent': UserAgent().random.strip(),
              'clientapplicationid':'quantum',
              'content-type':'application/json; charset=utf-8',
              'locale':'jp-jp'}
    store_lists = self.session.get(url, headers=headers).json()
    # print(store_lists)

    for store in store_lists['payload']['dealers']:
      _store = dict()
      if store['dealerUrl'] in self.visited_url: continue
      self.visited_url.append(store['dealerUrl'])

      _store['url_store'] = store['dealerUrl']
      if _store['url_store'] == '': _store['url_store'] = "REDACTED URL"

      _store['store_name'] = store['dealerName']
_store['address'] = store['address']['postalCode'] +' '+ store['address']['region'] + store['address']['cityName'] + store['address']['addressLine1']+ store['address']['addressLine2']

      maps_link = "REDACTED URL"+ 
_store['address']

      _store['lat'], _store['lon'] = '', ''



      _store['tel_no'] = store['generalContact']['phone1']

      try: _store['open_hours'] = store['generalOpeningHour'][0]['openFrom'] + '-' + store['generalOpeningHour'][0]['openTo']
      except IndexError: _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
      print(len(self.content))


# PRINT HTML TO FILE
# with open('_super_/res.html', 'w', encoding='utf8') as f:
#     f.write(page.html.html)