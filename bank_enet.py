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

class BankEnet(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/bank/enet'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.visited_stores = list()
        self.mng_bank = list()
        self.start = time.time()
        self.count = 0
        while True:
            print('offset:', self.count)
            self.seed = randint(360000000, 369999999)
            self.time_now = datetime.datetime.now().strftime('%Y%m%d%H%M')
            self.url = f"REDACTED URL"
            try:
                self.res = self.get_page(self.url)
                self.count += 500
                if self.res == 0:
                    break
                time.sleep(1)
            except:
                raise
            if True:
                pass
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
            try:
                if not store['external_code'].startswith('EN'):
                    continue
            except KeyError:
                continue
            if store['code'] in self.visited_stores:
                continue
            if len(store['details'][0]['dates']) > 1:
                continue
            self.visited_stores.append(store['code'])
            _store = dict()
            _store['url_store'] = "REDACTED URL"+ store['code']
            _store['store_name'] = store['name']
_store['address'] = store['postal_code'] + ' ' + store['address_name']
            _store['lat'], _store['lon'] = '', ''
            _store['tel_no'] = ''
            _store['open_hours'] = [x['value'] for x in store['details'][0]['texts'] if x['label'] == '営業時間'][0]
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
    BankEnet(True)
