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

class BentoChiyoda(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/bento/chiyoda'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.start = time.time()
        self.count = 1
        while True:
            self.url = f"REDACTED URL"
            self.data = self.get_page(self.url)
            self.count += 1
            if self.data == 0:
                break
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
        print(url)
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        store_lists = soup.find_all('table', 'listTable')
        for store in store_lists:
            _store = dict()
            _store['url_store'] = "REDACTED URL"
            _store['store_name'] = store.find('th', text='店舗名').find_next('td').text
_store['address'] = store.find('th', text='住所').find_next('td').text.replace('\u3000', ' ').replace('\n', '').replace('\t', '').replace('\r', '')
            maps_link = store.find('th', text='住所').find_next('td').find('a')['href']

            _store['lat'], _store['lon'] = '', ''
            _store['tel_no'] = store.find('th', text='電話番号').find_next('td').text.replace('\n', '').replace('\t', '').replace('\r', '')
            _store['open_hours'] = store.find('th', text='営業時間').find_next('td').text.replace('\u3000', '').replace('\n', '').replace('\t', '').replace('\r', '')
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
            self.save_data()
            print(len(self.content))
        return len(store_lists)

    def get_map_data(self, url):
        headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
        page = self.session.get(url, headers=headers, allow_redirects=True)
        return page

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    BentoChiyoda(True)

