import os

from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class CardealerSuzuki(CleaningData):
  def __init__(self, from_main=False):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'scapingteam-a2d07bd2f068.json'
    # Taruh semua variabel yang ada di luar class/methods disini
    self.file_name = '_cardealer_suzuki'
    self.session = HTMLSession()
    self.content = list()
    self.visited_page = list()

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

    prefs = soup.find_all('div', 'ct')
    for pref in prefs:
      while True:
        try:
          pref_page = self.session.get(pref.find('a')['href'], headers=headers)
          pref_soup = BeautifulSoup(pref_page.html.html, 'html.parser')
          break
        except: time.sleep(10)

      areas = pref_soup.find('table').find_all('a')
      for area in areas:
        while True:
          try:
            headers = {'user-agent': UserAgent().random.strip()}
            area_page = self.session.get(area['href'], headers=headers)
            area_soup = BeautifulSoup(area_page.html.html, 'html.parser')
            break
          except: time.sleep(10)

        store_lists = area_soup.find_all('div', 'name')
        for store in store_lists:
          code = store.find('a')['href'].split('/')[-2]
          if code in self.visited_page: continue

          self.content.append(self.get_data(store.find('a')['href']))
          print(len(self.content))
          self.visited_page.append(code)
          time.sleep(1)

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    while True:
      try:
        headers = {'user-agent': UserAgent().random.strip()}
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        break
      except:
        time.sleep(5)
    _store = dict()

    _store['url_store'] = url

    try:
      _store['store_name'] = soup.find('p', 'bl_headerSiteId_name').text
    except AttributeError:
      try:
        _store['store_name'] = soup.find('h1', 'bl_headerSiteId_name').text
      except AttributeError:
        _store['store_name'] = soup.find('p', 'bl_dealerHeaderSiteId_name').textaddress = soup.find('th', text='所在地').find_next_sibling('td')
    
_store['address'] = address.text.replace('アクセス詳細', '').replace('\n', '').replace(' ', '')

    _store['lat'], _store['lon'] = '',''

    _store['tel_no'] = soup.find('th', text='TEL').find_next_sibling('td').text.replace('\n', '').replace('\t',
                                                                                                          '').replace(
      '\r', '').replace('\u3000', ' ')

    open_hours = soup.find('th', text='営業時間').find_next_sibling('td').text.replace('\n', '').replace('\t','').replace(
      '\r', '').replace('\u3000', ' ').split('◆')[0].split('◇')[0].split('※')[0].split('TEL：')[0].split('TEL:')[0]
    if '住所' in open_hours: open_hours = ''
    _store['open_hours'] = open_hours

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page

if __name__ == '__main__':
    CardealerSuzuki(True)
