import re

from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
from scraping.scraping import CleaningData
from google.cloud import storage


class ParkingShouwapark(CleaningData):
  def __init__(self, from_main):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.file_name = '_parking_shouwapark'
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    count = 1
    while True:
      url = f"REDACTED URL"
      print(url)
      try:
        self.get_page(url)
      except StopIteration:
        break
      count += 1
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

    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    try:
      self.df_clean = self.clean_data(x)
    except:
      raise
    if True:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
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

    store_lists = soup.find_all('tr')
    print(len(store_lists))
    for store in store_lists[1:]:
      link = store.find('a')['href']
      self.content.append(self.get_data(link))

      print(len(self.content))
    if len(store_lists) == 1: raise StopIteration

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('h1').text
_store['address'] = soup.find('p', 'address').text.split('：')[-1].replace('\n', '').replace('\t', '').replace('\r',
                                                                                                                  '').replace(
      '\u3000', ' ')

    maps_link = "REDACTED URL"+ 
_store['address']

    _store['lat'], _store['lon'] = '', ''



    _store['tel_no'] = ''

    _store['open_hours'] = soup.find('th', text='基本料金').find_next_sibling('td').text.replace('\n', '').replace('\t',
                                                                                                               '').replace(
      '\r', '').replace('\xa0', '').replace(' ', '')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

if __name__ == '__main__':
    ParkingShouwapark(True)

