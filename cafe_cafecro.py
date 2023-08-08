import re

from fake_useragent import UserAgent
import time, datetime
from bs4 import BeautifulSoup
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import random, json
from scraping.scraping import CleaningData
try:
    from google.cloud import storage
except ModuleNotFoundError:
    pass

class CafeCafecro(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cafe/cafecro'.replace("/", "_")
        self.content = list()
        self.content = list()
        self.session = HTMLSession()
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
  If no, call get_data(url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        rand_seed = random.randint(610000000, 619999999)
        get_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
        if url == '':
            url = f"REDACTED URL"
        page =self.session.get(url, headers=headers)
        store_lists = json.loads(page.html.html)
        for store in store_lists['items']:
            _store = dict()
            _store['url_store'] = "REDACTED URL"+ store['code']
            _store['store_name'] = store['name'].replace('\u3000', '')
_store['address'] = store['address_name'].replace('\u3000', '').replace('\n', '')
            maps_link = "REDACTED URL"+ 
_store['address']

            _store['lat'], _store['lon'] = '', ''
            try:
                _store['tel_no'] = store['phone'].replace('-', '')
            except KeyError:
                _store['tel_no'] = ''
            _store['open_hours'] = store['details'][0]['texts'][0]['value'].replace('\n', '').replace('\u3000', '')
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
        self.save_data()
        print(len(self.content))
        if store_lists['count']['total'] > store_lists['count']['offset']:
            offset = int(store_lists['count']['offset']) + 50
            get_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
            rand_seed = random.randint(610000000, 619999999)
            url_new = f"REDACTED URL"
            self.get_page(url_new)

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CafeCafecro(True)

