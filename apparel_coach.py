import asyncio

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

class ApparelCoach(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main

    self.file_name = '/apparel/coach'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.content = list()
    self.start = time.time()
    # loop = asyncio.new_event_loop()
    self.PROXIES = {
      "REDACTED URL"
      'REDACTED URL}
    self.url = ["REDACTED URL" 'https://japan.coachoutlet.com/stores/']
    for x in self.url:

      self.get_page(x, x)
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

  def get_page(self, domain, url):
    """
Visit the link given from the gsheet,
see if there's data there.
If yes, scrape immediately.
If no, call self.get_data(self.url) and visit individual pages.
"""
    print(url)
    # self.session = AsyncHTMLSessionFixed()
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, proxies=self.PROXIES)
    time.sleep(0.5)
    # await page.html.arender(sleep=1)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    store_lists = soup.find('div', 'Directory-content').find_all('a')
    for store in store_lists:
      if 'tel' in store['href']: continue
      link = domain + store['href'].replace('..', '')
      try:
        if store['data-count'] == '(1)':
          self.content.append(self.get_data(link.replace('stores//', 'stores/')))
          self.save_data()
          print(len(self.content))
          time.sleep(0.5)
        else:
          self.get_page(domain, link.replace('stores//', 'stores/'))
      except KeyError:
        self.content.append(self.get_data(link.replace('stores//', 'stores/')))
        self.save_data()
        print(len(self.content))
        time.sleep(0.5)

  def get_data(self, url):
    """
Visit individual page,
see if you can scrape map latitude and longitude.
If no, visit map individually by calling get_map_data(self.url)
"""
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    while True:
      try:
        page = self.session.get(url, headers=headers, proxies=self.PROXIES)    # proxies=self.PROXIES)
        break
      except:
        time.sleep(1)
    # await page.html.arender()
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()
    _store['url_store'] = url
    _store['store_name'] = soup.find('h1').texttry:
      
_store['address'] = soup.find('address').text
    except AttributeError:
      try: 
_store['address'] = soup.select('span[itemprop=postalCode]')[0].text + soup.select('span[itemprop=addressRegion]')[0].text + soup.select('span[itemprop=addressLocality]')[0].text + soup.select('span[itemprop=streetAddress]')[0].text
      except IndexError: 
_store['address'] = soup.select('span[itemprop=postalCode]')[0].text + soup.select('span[itemprop=postalCode]')[0].parent.find_next_sibling('div').text
    _store['lat'], _store['lon'] = '',''
    try: _store['tel_no'] = soup.find('th', text=re.compile('電話番号')).find_next_sibling('td').text
    except: _store['tel_no'] = ''
    try: _store['open_hours'] = soup.select('meta[itemprop=openingHours]')[0]['content']
    except: _store['open_hours'] = ''
    _store['lat'], _store['lon'] = '',''
    _store['gla'] = ''
    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
    return _store

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  ApparelCoach(True)
