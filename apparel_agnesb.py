import re

from fake_useragent import UserAgent
import datetime, csv, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
from scraping.scraping import CleaningData
from google.cloud import storage


class ApparelAgnesb(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    
    start = time.time()
    url = "REDACTED URL"
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

    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    try:
      self.df_clean = self.clean_data(x)
          
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_apparel_agnesb.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_apparel_agnesb.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
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
    headers = {'user-agent': UserAgent().random.strip(), 'accept-language':'ja, ja-JP;q=0.9'}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    store_lists = soup.find_all('a', 'a-shoplist__link')
    for store in store_lists:
      link = "REDACTED URL"+ store.get('href').replace('../', '')
      self.content.append(self.get_data(link))

      print(len(self.content))

    try:
      next_page = soup.find('li', 'a-pagenation__page-next').find('a')['href'].replace('../', '')
      self.get_page("REDACTED URL"+ next_page)
    except AttributeError:
      pass


  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('h3').text.replace('\xa0', '').replace('\u3000', ' ')
_store['address'] = soup.find('dt', text='住所').find_next_sibling('dd').text.replace('\xa0', '').replace('\u3000', ' ')

    # gak ada map-nya, weird
    maps_link = "REDACTED URL"+ 
_store['address']

    _store['lat'], _store['lon'] = '', ''



    _store['tel_no'] = soup.find('dt', text='電話番号').find_next_sibling('dd').text.replace('\xa0', '').replace('\u3000', ' ')

    _store['open_hours'] = soup.find('dt', text='営業時間').find_next_sibling('dd').text.replace('\xa0', '').replace('\u3000', ' ')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store
        