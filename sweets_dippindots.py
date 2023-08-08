from requests.exceptions import ConnectionError
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class SweetsDippindots(CleaningData):
  def __init__(self, from_main):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.file_name = '_sweets_dippindots'
    self.session = HTMLSession()
    self.content = list()

    url = "REDACTED URL"
    start = time.time()
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start)/60, " minute(s) ============")

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
    print(url)
    headers = {
      'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}

    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')


    store_lists = soup.find_all('tr')
    for store in store_lists:
      link = store.find('a')['href']
      self.content.append(self.get_data(link))

      print(len(self.content))
      time.sleep(1)

    try:
      next_button = "REDACTED URL"+ soup.find('a', {'class':'next'})['href']
      self.get_page(next_button)
    except TypeError:
      pass

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individuually by calling get_map_data(url)
    '''
    print(url)
    headers = {
      'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
    page = self.session.get(url, headers=headers)

    soup = BeautifulSoup(page.html.html, 'html.parser')
    details = soup.find_all('dd')

    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('h3', 'ttl_h3_directshop').text.replace('\u3000', '')
_store['address'] = details[0].text.replace('(地図)', '')

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = details[1].text.replace('-', '')

    _store['open_hours'] = ''

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer':"REDACTED URL"
              'sec-ch-ua':'"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
              'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
    try:
      page = self.session.get(url, headers=headers, allow_redirects=True)
      return page
    except ConnectionError:
      return ''
        
if __name__ == '__main__':
  SweetsDippindots(True)