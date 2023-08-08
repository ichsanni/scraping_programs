import csv
import random
import asyncio
from aiohttp import ClientSession
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

class ShoppingConveniSeven(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = 'shopping/conveni/seven2'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.visited_stores = list()
        self.domain = 'https://www.e-map.ne.jp/p/711map/'
        self.PROXIES = {'REDACTED URL,
                        'REDACTED URL}
        self.start = time.time()
        nama_file = ['koordinat_5km']
        for nama in nama_file:
            with open(f'jp-cities-coordinates/{nama}.csv', 'r', encoding='utf8') as f:
                reader = csv.DictReader(f)
                reader = list(reader)
                for city in reader:
                    # if int(city['index']) < 13300: continue
                    print('--', city['index'])
                    self.get_store_lists('?' + str(city['lat']) + ' ' + str(city['lon']))
                    if int(city['index'])%100 == 0: self.save_data()
        x = pd.DataFrame(self.content)
        if len(x) == 0:
            raise ValueError('Isinya ra ono.')
        
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
                self.df_clean.to_csv(f'D:/dags/csv/ichsan_{self.file_name}.csv', index=False)
            else:
                client = storage.Client()
                bucket = client.get_bucket('scrapingteam')
                bucket.blob(f'rokesumaalternative/ichsan/{self.file_name}.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

    def __str__(self) -> str:
        return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

    def encode_url_sevel(self, _lat, _lon, page=0, index=1):
        """
  encode url and insert latitude and longitude wanted
  """
        cnt = 700
        url_page = f'zdcemaphttp.cgi?target=http%3A%2F%2F127.0.0.1%2Fcgi%2Fnkyoten.cgi%3F%26cid%3D711map%26opt%3D711map%26' \
                   f'pos%3D1%26cnt%3D{cnt}%26enc%3DEUC%26lat%3D{_lat}%26lon%3D{_lon}%26latlon%3D%26' \
                   f'jkn%3D(COL_02%3ADT%3ALTE%3ASYSDATE%20AND%20COL_12%3A1)%26' \
                   f'rad%3D5000000%26knsu%3D{cnt}%26exkid%3D%26hour%3D1%26cust%3D%26exarea%3D%26polycol%3D&zdccnt={index}&enc=EUC&encodeflg=0'
        return url_page

    def get_store_lists(self, url, current_page=int(0)):
        headers = {'user-agent': UserAgent().random.strip()}
        index = random.randint(1, 25)
        latlon = re.findall('\\d{2,3}\\.\\d{2,}', url)
        encoded = self.encode_url_sevel(latlon[0], latlon[1], current_page, index=index)
        url_emap = 'https://www.e-map.ne.jp/p/711map/' + encoded
        while True:
            try:
                page =self.session.get(url_emap, headers=headers, timeout=3)    # proxies=self.PROXIES,
                store_lists = page.content.decode('euc-jp').split('=')[1].split('\\n')
                break
            except Exception as e:
                print(e.args)
                time.sleep(1)
        for store in store_lists:
            store = store.split('\\t')
            if store[0] in self.visited_stores:
                continue
            try:
                store[8]
            except IndexError:
                continue
            self.visited_stores.append(store[0])
            _store = dict()
            _store['url_store'] = f'http://www.e-map.ne.jp/p/711map/dtl/{store[0]}/?&p_s1=40000'
            _store['store_name'] = store[8]
           
            _store['address'] = store[12] + ' ' + store[13]
            _store['lat'] = ''
            _store['lon'] = ''
            _store['tel_no'] = store[14]
            _store['open_hours'] = '24時間営業' if str(store[15]) == '' and str(store[16]) == '' else str(store[15]) + '~' + str(store[16])
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            self.content.append(_store)
        print(len(store_lists) - 2, '||', len(self.visited_stores))

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    ShoppingConveniSeven(True)

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
