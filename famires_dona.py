from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try: from google.cloud import storage
except ModuleNotFoundError: pass


class FamiresDona(CleaningData):
  def __init__(self, from_main=False):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
    self.get_page(url, from_main)
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
        self.df_clean.to_csv('D:/dags/csv/_famires_dona.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_famires_dona.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url, from_main=True):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    main_url = "REDACTED URL"
    store_lists = soup.select('img[alt="店舗詳細"]')
    for store in store_lists:
      link = main_url + store.parent['href'] if 'towa-net.co.jp' not in store.parent['href'] else store.parent['href']
      self.content.append(self.get_data(link))

      if from_main: self.save_data()
      print(len(self.content))

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    try: _store['store_name'] = soup.find('div', 'dona-pageTitle').text.replace('\n', '').replace('\t', '').replace('\r',
                           '').replace('\u3000', ' ')
    except AttributeError: _store['store_name'] = soup.find('div', 'duckyduck-pageTitle').find('h1').text.replace('\n',
                           '').replace('\t', '').replace('\r', '').replace('\u3000', ' ')
_store['address'] = soup.find('th', text='住所').find_next('td').text.replace('\n', '').replace('\t', '').replace(
      '\r', '').replace('\u3000', '')


    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup.find('th', text='TEL').find_next('td').text.replace('\n', '').replace('\t', '').replace(
      '\r', '').replace('\u3000', ' ')

    _store['open_hours'] = soup.find('th', text='営業時間').find_next('td').text.replace('\n', '').replace('\t',
    '').replace('\r', '').replace('\u3000', ' ')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page

  def save_data(self):
    df = pd.DataFrame(self.content)
    df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                                           '-', '-', '-', 'address', 'url_store', 'url_tenant',
                                           'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
    df.to_csv('D:/dags/csv/_famires_dona.csv', index=False)

if __name__ == '__main__':
  FamiresDona(True)