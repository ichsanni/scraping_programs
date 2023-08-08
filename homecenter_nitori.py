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

class HomecenterNitori(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/homecenter/nitori'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        for x in range(1, 48):
            self.timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
            self.index = 0
            self.limit = 500
            self.offset = self.limit * self.index
            self.address = x if len(str(x)) == 2 else '0' + str(x)
            self.url = f"REDACTED URL"
            self.item_count = self.get_page(self.url)
            print(len(self.content))
            self.index += 1
            time.sleep(3)
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
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers)
        store_lists = json.loads(page.html.html)
        for store in store_lists['items']:
            _store = dict()
            _store['url_store'] = "REDACTED URL"+ str(store['code'])
            _store['store_name'] = store['name']
_store['address'] = '〒' + store['postal_code'][0:3] + '-' + store['postal_code'][3:] + ' ' + store['address_name'].split('<')[0]
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''

            try:
                _store['tel_no'] = store['phone'].replace('-', '')
            except KeyError:
                _store['tel_no'] = ''
            texts = store['details'][0]['texts']
            open_hours = ', '.join([''.join([x['label'], x['value']]) for x in texts if '営業時間' in x['label']])
            rmv_tags = re.sub('<[^>]*>', '', open_hours)
            _store['open_hours'] = rmv_tags
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
            self.save_data()
        return len(store_lists)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    HomecenterNitori(True)

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
