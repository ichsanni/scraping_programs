import json

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

class ApparelGu(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/apparel/gu'.replace("/", "_")
        self.content = list()
        self.rand_agent = {'user-agent': UserAgent().random.strip()}
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        for x in range(0, 500, 100):
            self.url = "REDACTED URL"+ str(x)
            self.y = self.get_page(self.url)
            if self.y == 0:
                break
            time.sleep(2)
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
  If no, call get_data(url) and visit individual pages.
  """
        try:
            page =self.session.get(url, headers=self.rand_agent)
        except ConnectionError:
            time.sleep(2)
            page =self.session.get(url, headers=self.rand_agent)
        store_json = json.loads(page.html.html)
        domain = "REDACTED URL"
        for store in store_json['result']['stores']:
            _store = dict()
            if store['store_name'] == 'オンラインストア':
                continue
            else:
                print(domain + str(store['store_id']))
                _store['url_store'] = domain + str(store['store_id'])
                _store['store_name'] = store['store_name'].split('ジーユー')[-1].replace('\u3000', '')
_store['address'] = '〒' + store['postcode'] + ' ' + store['address'].replace('\u3000', '')
                _store['lat'], _store['lon'] = '',''
                _store['tel_no'] = store['phone'].replace('-', '').replace('\u3000', '')
                _store['open_hours'] = store['wd_open_at'] + '-' + store['wd_close_at']
                _store['gla'] = ''
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                # print(_store)
                self.content.append(_store)
                self.save_data()
                print(len(self.content))
        return len(store_json['result']['stores'])

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    ApparelGu(True)

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
