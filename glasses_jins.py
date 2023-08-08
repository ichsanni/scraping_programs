import asyncio
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

class GlassesJins(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/glasses/jins'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.store_links = list()
        self.start = time.time()
        page_count = 1
        while True:
            try:
                self.get_page(page_count)
                page_count += 1
            except StopIteration: break
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

    def get_page(self, page_count=0):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call get_data(self.url) and visit individual pages.
  """
        params = {
            'grp': 'jins_shop',
            'vo': 'mbml',
            'json': '1',
            'poi_status': '1',
            'start': page_count,
            'entref': '1',
            'srt': 'closed_date:d,citycode,poi_name_yomi',
        }
        headers = {
            'authority': 'lbs.mapion.co.jp',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,id;q=0.8',
            'referer': "REDACTED URL"
        }

        response = self.session.get("REDACTED URL" params=params, headers=headers)
        store_lists = json.loads(response.html.html.replace('(', '').replace(');', ''))
        try: store_lists['mbml']['PoiList']['Poi']
        except KeyError: raise StopIteration
        for details in store_lists['mbml']['PoiList']['Poi']:
            try:
                _store = dict()
                _store['url_store'] = f"REDACTED URL"
                _store['store_name'] = details['name']
_store['address'] = details['full_address'].replace('\u3000', '')
                _store['lat'], _store['lon'] = '',''

                try:
                    _store['tel_no'] = details['tel'].replace('-', '')
                except KeyError:
                    _store['tel_no'] = ''
                try:
                    _store['open_hours'] = details['business_hours_text'].replace('\n', '').replace('\u3000', '').replace('<br />', '')
                except KeyError:
                    _store['open_hours'] = ''
                _store['gla'] = 'Null'
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                self.content.append(_store)
                print(len(self.content))
            except KeyError:
                pass
            self.save_data()


    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    GlassesJins(True)


