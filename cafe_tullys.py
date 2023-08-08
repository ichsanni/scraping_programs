import datetime, csv, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import json, re
import pandas as pd
try:
  from google.cloud import storage
  from scraping.scraping import CleaningData
except ModuleNotFoundError: pass


class CafeTullys(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    self.file_name = '_cafe_tullys'
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = f"REDACTED URL"
    self.get_page(url)

    end = time.time()
    print("============ ", (end - start)/60, " minute(s) ============")

    client = storage.Client()
    bucket = client.get_bucket('scrapingteam')
    x = pd.DataFrame(self.content)
    
    x['url_tenant'] = None

    # PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA

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

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
    
  def __str__(self) -> str:
      return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {
      'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    domain = "REDACTED URL"

    store_lists = soup.find_all('a', 'store__link')
    for store in store_lists:
      link = domain + store['href']
      self.content.append(self.get_data(link))

      print(len(self.content))
      self.save_data()
      time.sleep(0.5)

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

    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('span', 'page-title__text').text
_store['address'] = re.sub('\s', '', soup.find('div', 'address__detail').text)

    maps_link = "REDACTED URL"+ 
_store['address'] + _store['-']

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = re.sub('\s', '', soup.find('a', 'phone-section__link').text)

    _store['open_hours'] = re.sub('\s', '', soup.find('div', 'business-hour__detail').text)

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store
    

if __name__ == '__main__':
  CafeTullys(True)