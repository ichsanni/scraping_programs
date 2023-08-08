from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass

class HomecenterJuntendo(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main

    self.file_name = '/homecenter/juntendo'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.content = list()
    self.url = "REDACTED URL"
    self.start = time.time()
    self.get_page(self.url)
    self.end = time.time()

    x = pd.DataFrame(self.content)
    if len(x) == 0:
      raise ValueError('Empty df')

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
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    """
Visit the link given from the gsheet,
see if there's data there.
If yes, scrape immediately.
If no, call self.get_data(self.url) and visit individual pages.
"""
    headers = {'user-agent': UserAgent().random}
    page = self.session.get(url, allow_redirects=True, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    domain = "REDACTED URL"
    area_list = soup.find_all('area')
    for area in area_list:
      link = domain + area['href'].replace('list', 's')
      self.get_data(link)

  def get_data(self, url):
    """
Visit individual page,
see if you can scrape map latitude and longitude.
If no, visit map individuually by calling self.get_map_data(self.url)
"""
    print(url)
    page = self.session.get(url)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    store_lists = soup.find_all('table', {'class': 'shopdetails'})
    for store in store_lists:
      try:
        details = store.find('table').find_all('td')[0].text.strip().replace('\u3000', '').replace(' ', '').split('\n')
        if len(details) < 2: details = store.find('table').find_all('td')[1].text.strip().replace('\u3000', '').replace(' ', '').split('\n')
        _store = dict()
        _store['url_store'] = url
        _store['store_name'] = store.find('th').textprint(details)
        address = ''.join(details).split('〒')[-1].split('（')[0].split('(')[0]
        
_store['address'] = address.replace('（地図）', '')
        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        try: _store['tel_no'] = details[-1].split(':')[1].replace('-', '').replace('FAX', '')
        except IndexError: _store['tel_no'] = ''
        _store['open_hours'] = details[0].replace('営業時間', '')
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        self.content.append(_store)
        self.save_data()
        print(len(self.content))
      except AttributeError:
        continue

async def get_map_data(self, url_map):
  headers = {'referer': "REDACTED URL"Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
  page =self.session.get(url_map, headers=headers, allow_redirects=True)
  await page.html.arender(timeout=0)
  return page

def save_data(self):
  if self.from_main:
    df = pd.DataFrame(self.content)
    df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
    df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  HomecenterJuntendo(True)

class AsyncHTMLSessionFixed(AsyncHTMLSession):
  """
  pip3 install websockets==6.0 --force-reinstall
  """

  def __init__(self, **kwargs):
    super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
    self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])

  @property
  async def browser(self):
    if not hasattr(self, "_browser"):
      self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
                                             handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

    return self._browser
