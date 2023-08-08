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

class 100kinSilk(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main

    self.file_name = '/100kin/silk'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.content = list()
    self.start = time.time()
    self.get_page()
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

  def get_page(self, url=''):
    """
Visit the link given from the gsheet,
see if there's data there.
If yes, scrape immediately.
If no, call self.get_data(url) and visit individual pages.
"""
    print(url)
    data = {'keyword': 'シルク', 'prefecture_id': '', 'address1': '', 'action_user_shop_index': 'true'}
    headers = {'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
    if url:
      try:
        page =self.session.get(url, headers=headers)
      except ConnectionError:
        time.sleep(3)
        page =self.session.get(url, headers=headers)
    else:
      page =self.session.get("REDACTED URL" headers=headers, data=data)
      print(page.url)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    domain = "REDACTED URL"
    store_lists = soup.find_all('div', 'info')
    for store in store_lists:
      link = store.parent['href']
      self.content.append(self.get_data(link))
      self.save_data()
      print(len(self.content))

  def get_data(self, url):
    """
Visit individual page,
see if you can scrape map latitude and longitude.
If no, visit map individually by calling self.get_map_data(url)
"""
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page =self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()
    _store['url_store'] = url
    _store['store_name'] = soup.find('h1').text.replace(soup.find('h1').find('span').text, '')
_store['address'] = soup.find('dt', text='住所').parent.find('dd').text.replace('GoogleMapで確認する', '').replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
    # maps_link = "REDACTED URL"+ 
_store['address']
    # maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
    #
    #
    # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
    # ## print(coords)
    # if coords:
    #   _store['lat'] = coords[0].split(',')[0].replace('@', '')
    #   _store['lon'] = coords[0].split(',')[1]
    # else:
    #   try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
    #   except:
    #     time.sleep(5)
    #     maps_link = "REDACTED URL"+ 
_store['address']
    #     maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
    #     try:
    #       location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
    #       lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
    #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
    #     except:
    #       try:
    #         lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
    #         _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
    _store['lat'], _store['lon'] = '', ''
    _store['tel_no'] = ''
    _store['open_hours'] = soup.find('dt', text='営業時間').find_next_sibling('dd').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '').split('新型')[0]
    _store['gla'] = ''
    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
    print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page =self.session.get(url, headers=headers, allow_redirects=True)
    return page

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  100kinSilk(True)

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
