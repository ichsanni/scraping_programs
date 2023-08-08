import csv
import json
import random
from json import JSONDecodeError

import requests
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

class CardealerHonda(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/cardealer/honda'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.page_code = list()
        self.start = time.time()
        try:
            with open('jp-cities-coordinates/jp-coord-complete.csv', 'r', encoding='utf8') as f:
                reader = csv.DictReader(f)
                index_city = 1
                for city in reader:
                    print('city:', index_city, city['region'])
                    data = {'latitude': city['lat'], 'longitude': city['lng'], 'requestType': '0', 'searchMode': 'local', 'isNotZoom': 'true', 'isCondition': 'false', 'version': random.randint(3, 9), 'isCancelAccesslog': 'true'}
                    url = "REDACTED URL"
                    self.get_page(url, data)
                    time.sleep(3)
                    index_city += 1
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

    def get_page(self, url, data):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call self.get_data(url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        try:
            page =self.session.post(url, headers=headers, data=data)
        except requests.exceptions.SSLError:
            time.sleep(5)
            page =self.session.post(url, headers=headers, data=data)
        except requests.exceptions.ProxyError:
            time.sleep(5)
            page =self.session.post(url, headers=headers, data=data)
        domain = "REDACTED URL"
        try:
            store_lists = json.loads(page.html.html)
        except JSONDecodeError:
            return 0
        print(len(store_lists['shopInfos']))
        for store in store_lists['shopInfos']:
            code = str(store['hojinCd']) + str(store['kanbanCd']) + str(store['kyotenCd'])
            if code not in self.page_code:
                link = domain + code + '/op_auto/'
                store_data = self.get_data(link, store['latitude'], store['longitude'])
                if store_data != '':
                    self.content.append(store_data)
                    self.save_data()
                    print(len(self.content))
                    time.sleep(1)
                self.page_code.append(code)
            else:
                continue

    def get_data(self, url, lat, lon):
        """
  Visit individual page,
  see if you can scrape map latitude and longitude.
  If no, visit map individuually by calling get_map_data(url)
  """
        print(url)
        headers = {'user-agent': UserAgent().random.strip()}
        try:
            page =self.session.get(url, headers=headers)
        except requests.exceptions.SSLError:
            time.sleep(5)
            page =self.session.get(url, headers=headers)
        except requests.exceptions.ProxyError:
            time.sleep(5)
            page =self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()
        _store['url_store'] = url
        try:
            name = soup.find('h1').text
            _store['store_name'] = name.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
        except AttributeError:
            link = url.replace('/auto/', '/ucar/')
            store_data = self.get_data(link, lat, lon)
            return store_datatry:
            
_store['address'] = soup.find('div', 'txt-box').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
        except AttributeError:
            return ''
        _store['lat'], _store['lon'] = '',''

        tel_no = soup.find('span', text=re.compile('電話番号')).text.split('：')[1].replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
        _store['tel_no'] = tel_no
        _store['open_hours'] = soup.find('span', text=re.compile('営業時間')).text.split('：')[1].replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        return _store

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    CardealerHonda(True)

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
