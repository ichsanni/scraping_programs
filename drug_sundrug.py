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

class DrugSundrug(CleaningData):

    def __init__(self, from_main=False):
        self.from_main = from_main
 
        self.file_name = '/drug/sundrug'.replace("/", "_")
        self.content = list()
        self.session = HTMLSession()
        self.content = list()
        self.visited_stores = list()
        self.PROXIES = {"REDACTED URL"
                        'REDACTED URL}
        self.start = time.time()
        with open('jp-cities-coordinates/jp_coord.csv', 'r', encoding='utf8') as f:
            reader = csv.DictReader(f)
            for prefs in reader:
                print(prefs['city'])
                self.url = f"REDACTED URL"\
                   f'%3F%26key%3D40nQMP9FlgcvBpnArvB3nAbf9nmgDPBiidc5S2XuzmAJfhlzTPrxkzTHrxlzTtng0XBCoQlv8NoQszFMpRzzT5%26' \
                   f'cid%3Dsundrug%26opt%3Dsundrug%26pos%3D1%26cnt%3D1000%26enc%3DEUC%26' \
                   f'lat%3D{prefs["lat"]}%26lon%3D{prefs["lng"]}%26latlon%3D%26jkn%3D%26' \
                   f'rad%3D100000%26knsu%3D1000%26exkid%3D%26hour%3D1%26cust%3D%26exarea%3D%26polycol%3D&zdccnt={random.randint(1, 25)}&enc=EUC&encodeflg=0'
                self.get_area(self.url)
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

    def get_area(self, url):
        """
  Visit the link given from the gsheet,
  see if there's data there.
  If yes, scrape immediately.
  If no, call self.get_data(self.url) and visit individual pages.
  """
        headers = {'user-agent': UserAgent().random.strip()}
        while True:
            try:
                page = self.session.get(url, headers=headers)
                store_lists = page.content.decode('euc-jp').split('=')[1].split('\\n')
                break
            except:
                time.sleep(3)
        for store in store_lists:
            store = store.split('\\t')
            if store[0] in self.visited_stores:
                continue
            try:
                store[8]
            except IndexError:
                continue
            if '薬局' in store[10]: continue
            self.visited_stores.append(store[0])
            _store = dict()
            _store['url_store'] = url
            _store['store_name'] = store[10]
_store['address'] = store[13]

            _store['lat'], _store['lon'] = '', ''

            _store['tel_no'] = store[15]
            _store['open_hours'] = store[17]
            _store['gla'] = ''
            _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
            # print(_store)
            self.content.append(_store)
        print(len(self.content))
        self.save_data()

    def save_data(self):
        if self.from_main:
            df = pd.DataFrame(self.content)
            df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
            df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
    DrugSundrug(True)

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
