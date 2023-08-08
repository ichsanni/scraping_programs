import csv
import random

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

class CardealerMazda(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cardealer/mazda'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.visited_links = list()
        self.start = time.time()
        try:
            with open('jp-cities-coordinates/koordinat_20km.csv', 'r', encoding='utf8') as f:
                reader = csv.DictReader(f)
                for city in reader:
                    print('city:', city['index'])
                    self.url = f"REDACTED URL"
                    self.get_page(self.url)
                    time.sleep(3)
        except:
            raise
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
        page =self.session.get(url, headers=headers)
        store_json = page.json()
        for store in store_json['Shops']:
            if [store['ShopId'], store['DealerId']] not in self.visited_links:
                _store = dict()
                _store['url_store'] = f"REDACTED URL"
                _store['store_name'] = store['ShopName']if store['Address']['Prefecture'] == store['Address']['City']:
                    
_store['address'] = store['Address']['Zip'] + store['Address']['Prefecture'] + store['Address']['Address1'] + store['Address']['Address2']
                else:
                    
_store['address'] = store['Address']['Zip'] + store['Address']['Prefecture'] + store['Address']['City'] + store['Address']['Address1'] + store['Address']['Address2']
                maps_link = "REDACTED URL"+ 
_store['address']

                _store['lat'], _store['lon'] = '', ''

                _store['tel_no'] = store['Contact']['PhoneNumber']
                _store['open_hours'] = store['Contact']['OpenTime'].replace('<br />', '')
                _store['gla'] = ''
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                self.content.append(_store)
                self.visited_links.append([store['ShopId'], store['DealerId']])
        self.save_data()
        print(len(self.content))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CardealerMazda(True)

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
