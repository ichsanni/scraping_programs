from fake_useragent import UserAgent

import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class ParkingTrustpark(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    page_count = 0
    while True:
      url = f"REDACTED URL"
      print(url)
      try:
        self.get_page(url)
      except StopIteration:
        break
      page_count += 1
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
      bucket.blob('/ichsan/_parking_trustpark.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_parking_trustpark.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
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

    store_lists = soup.find('div', 'shisetu_itiran').find_all('tr')[::2]
    for store in store_lists:
      at_parking = False
      if 'at-parking' in store.find('a')['href']:
        at_parking = True
        link = store.find('a')['href']
      else:
        link = "REDACTED URL"+ store.find('a')['href']
      data = self.get_data(link, at_parking)
      if data:
        self.content.append(data)

        print(len(self.content))
        time.sleep(1)

    if len(store_lists) == 0: raise StopIteration

  def get_data(self, url, at_parking=False):
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

    try: _store['store_name'] = soup.find('td', text="駐車場名称").find_next_sibling('td').text if at_parking else soup.find('h2').text
    except AttributeError: return
_store['address'] = soup.find('td', text="駐車場所在地").find_next_sibling('td').text if at_parking else soup.find('div', text='住所').find_next_sibling('div').text

    maps_link = "REDACTED URL"+ 
_store['address']

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = '' if at_parking else soup.find('div', text='TEL').find_next_sibling('div').text.replace('\xa0', '').replace('-','')

    _store['open_hours'] = '' if at_parking else soup.find('div', text='営業時間').find_next_sibling('div').text.replace('\xa0', '')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store


