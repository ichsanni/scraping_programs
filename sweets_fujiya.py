import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class SweetsFujiya(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    url = "REDACTED URL"
    start = time.time()
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
      bucket.blob('/ichsan/_sweets_fujiya.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')
    except:
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_sweets_fujiya.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')
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
    page = self.session.get(url)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    domain = "REDACTED URL"
    store_lists = soup.find_all('p', {'class': "shopName"})
    for store in store_lists:
      link = domain + store.find('a')['href']
      self.content.append(self.get_data(link))

      print(len(self.content))

    # kunjungi link yang ada di next_button
    try:
      next_button_link = domain + soup.find('a', {'id': 'm_nextpage_link'})['href']
      self.get_page(next_button_link)
    except TypeError:
      pass

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individuually by calling get_map_data(url)
    '''
    print(url)
    page = self.session.get(url)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('h1').text
_store['address'] = soup.find('th', text='郵便番号').find_next('td').text.strip() + ' ' + soup.find('th',
                                                                                                    text='住所').find_next(
      'td').text.strip()

    maps_link = "REDACTED URL"+ 
_store['address']

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup.find('span', 'telLinkNum').text.strip()

    try:
      _store['open_hours'] = " ".join(
        soup.find('th', text='営業時間').find_next('td').get_text(strip=True, separator=" ").split())
    except AttributeError:
      _store['open_hours'] = ' '

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

