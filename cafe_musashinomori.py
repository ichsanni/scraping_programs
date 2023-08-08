import json

import urllib3
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

class CafeMusashinomori(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cafe/musashinomori'.replace("/", "_")
        self.content = list()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = HTMLSession()
        self.pg_code = list()
        self.content = list()
        self.start = time.time()
        self.index = 0
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
  If no, call get_data(self.url) and visit individual pages.
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        page =self.session.get(url, headers=headers, verify=False)
        domain = "REDACTED URL"
        home_json = json.loads(page.html.html)
        for store in home_json['items']:
            if 'むさしの森' in store['name']:
                self.pg_code.append(store['key'])
                _store = dict()
                _store['url_store'] = domain + store['key']
                _store['store_name'] = store['name']
_store['address'] = store['address'].replace('\u3000', '')
                maps_link = "REDACTED URL"+ 
_store['address']

                _store['lat'], _store['lon'] = '', ''
                try:
                    _store['tel_no'] = store['電話番号']
                except KeyError:
                    _store['tel_no'] = ''
                _store['open_hours'] = ''
                _store['gla'] = ''
                _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
                # print(_store)
                self.content.append(_store)
                self.save_data()
                print(len(self.content))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CafeMusashinomori(True)
