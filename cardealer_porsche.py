from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
import requests
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass

class CardealerPorsche(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main

    self.file_name = '/cardealer/porsche'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.visited = list()
    self.start = time.time()
    self.url = "REDACTED URL"
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
    headers = {'user-agent': UserAgent().random.strip()}
    page =self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    content = list()
    store_lists = soup.find('table', 'normalTable').find_all('a')
    for store in store_lists:
      try:
        link = ''.join(re.findall('(https?://www.porsche[^/]*/)(dealers/[^/]*/?)?', store['href'])[0])
      except IndexError:
        continue
      try:
        if link not in self.visited:
          self.visited.append(link)
          data = self.get_data(link)
          if data:
            self.content.append(data)
            self.save_data()
            print(len(self.content))
            time.sleep(0.5)
      except requests.exceptions.ConnectionError:
        continue

  def get_data(self, url):
    """
Visit individual page,
see if you can scrape map latitude and longitude.
If no, visit map individually by calling get_map_data(self.url)
"""
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    # if 'chofu' in url:
    #   url = "REDACTED URL"
    #   print(url)
    page =self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()
    _store['url_store'] = url
    try: _store['store_name'] = soup.find('p', 'name').text
    except AttributeError: return
_store['address'] = soup.find('p', 'address').text.replace('\n', '').replace(' ', '').replace('\xa0', '').split('TEL')[0]

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup.find('p', 'tel').text.replace('TEL：', '')
    _store['open_hours'] = re.findall('営業時間 :[^\n]*\n?', page.html.html)[0].replace('営業時間 : ', '').replace('\n', '').replace('<br>', '').replace('\u3000', '')
    _store['gla'] = ''
    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
    return _store

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  CardealerPorsche(True)

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
