from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage

class ApparelAbahouse(CleaningData):
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
      bucket.blob('/ichsan/_apparel_abahouse.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_apparel_abahouse.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
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
    soup = BeautifulSoup(page.html.html, 'html.parser')

    store_lists = soup.find_all('table', 'cellContent')
    for store in store_lists:
      if store.find_all('td')[0].text.startswith('アバハウス'):
        _store = dict()

        _store['url_store'] = "REDACTED URL"

        _store['store_name'] = store.find('th', text='店舗名').find_next_sibling('td').text.replace(' ', '').replace('\u3000', '')
_store['address'] = store.find('th', text='住所').find_next_sibling('td').text.replace('\n', '').replace(' ', '').replace('\u3000', '')
        if not 
_store['address'].startswith('〒'): continue

        maps = self.get_map_data("REDACTED URL"+ 
_store['address'])


        # print(lat)

        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = store.find('th', text='電話番号').find_next_sibling('td').text

        _store['open_hours'] = ''

        _store['gla'] = ''

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        # print(_store)
        self.content.append(_store)

        print(len(self.content))
        time.sleep(1)

    try:
      next_button = "REDACTED URL"+ soup.find('li', 'next').find('a')['href']
      self.get_page(next_button)
    except AttributeError:
      pass

  def get_map_data(self, url):
    headers = {'referer':"REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)
    time.sleep(1)
    return page