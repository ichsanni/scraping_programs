from fake_useragent import UserAgent
import re
import datetime, csv, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import AsyncHTMLSession, HTMLSession
import pandas as pd
from scraping.scraping import CleaningData
from google.cloud import storage
import pyppeteer, asyncio

class ApparelKangol(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = ''
    self.content = list()
    self.map_html = ''
    
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
      bucket.blob('/ichsan/_apparel_kangol.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_apparel_kangol.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
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
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    self.session = HTMLSession()
    page = self.session.get(url, headers=headers)
    # await page.html.arender(timeout=0)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    store_lists = soup.find_all('li', 'block-shoplist-shop')
    for store in store_lists:
      _store = dict()

      _store['url_store'] = url

      _store['store_name'] = store.find('p', 'block-shoplist-shop-title').text
_store['address'] = store.find('p', 'block-shoplist-shop-addr').text

      maps_link = "REDACTED URL"'+ 
_store['address']
      print(maps_link)

      _store['lat'], _store['lon'] = '', ''

      _store['tel_no'] = store.find('p', 'block-shoplist-shop-tel').find('span', 'num').text

      _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      print(_store)
      self.content.append(_store)

      print(len(self.content))
      time.sleep(0.3)


if __name__ == '__main__':
  ApparelKangol(True)