import csv
from random import randint

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

class GyudonMatsuya(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/gyudon/matsuya'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.visited_stores = list()
        self.mng_bank = list()
        self.pref = 0
        self.start = time.time()
        with open('jp-cities-coordinates/prefektur.csv', 'r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.pref += 1
                self.count = 0
                while True:
                    print('name:', self.pref, row['nama'], 'offset:', self.count)
                    self.seed = randint(360000000, 369999999)
                    self.time_now = datetime.datetime.now().strftime('%Y%m%d%H%M')
                    self.url = f"REDACTED URL"
                    self.res = self.get_page(self.url)
                    self.count += 500
                    if self.res == 0:
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
        headers = {'user-agent': UserAgent().random.strip()}
        store_lists =self.session.get(url, headers=headers).json()
        offset = store_lists['count']['offset']
        total = store_lists['count']['total']
        print(store_lists['count'])
        store_lists = store_lists['items']
        for store in store_lists:
            if store['code'] in self.visited_stores:
                continue
            self.visited_stores.append(store['code'])
            if '松屋' not in store['name']:
                continue
            _store = dict()
            _store['url_store'] = "REDACTED URL"+ store['code']
            _store['store_name'] = store['name']
_store['address'] = store['postal_code'] + ' ' + store['address_name']
            _store['lat'], _store['lon'] = '', ''
            _store['tel_no'] = ''
            try:
                open_hours = [x['value'] for x in store['details'][0]['texts'] if x['label'] == '営業時間（日）'][0].lower().replace('br', '').replace('<', '').replace('>', '')
                _store['open_hours'] = open_hours.replace('\n', '').replace('\r', '').replace('\t', '').replace('\xa0', ' ')
            except IndexError:
                _store['open_hours'] = ''
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
        self.save_data()
        print(len(self.content))
        if total < 500:
            return 0
        if offset > total:
            return 0
        return len(store_lists)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    GyudonMatsuya(True)

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
