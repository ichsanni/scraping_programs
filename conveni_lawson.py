import csv, random
from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
from requests.exceptions import ProxyError, ChunkedEncodingError
from requests_html import HTMLSession, AsyncHTMLSession
import pyppeteer
import pandas as pd
import re
from scraping.scraping import CleaningData
try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass

class ShoppingConveniLawson(CleaningData):

  def __init__(self, from_main=False):
    self.from_main = from_main

    self.file_name = 'shopping/conveni/lawson'.replace("/", "_")
    self.content = list()
    self.session = HTMLSession()
    self.visited_stores = list()
    self.PROXIES = {}
    self.start = time.time()
    try:
      with open('jp-cities-coordinates/koordinat_20km.csv', 'r', encoding='utf8') as f:
        reader = csv.DictReader(f)
        for city in reader:
          print('--', city['index'])
          url = f"https://www.e-map.ne.jp/p/lawson/zdcemaphttp.cgi?target=http%3A%2F%2F127.0.0.1%2Fcgi%2Fnkyoten.cgi%3F%26cid%3Dlawson%26opt%3Dlawson%26pos%3D1%26cnt%3D1000%26enc%3DEUC%26lat%3D{city['lat']}%26lon%3D{city['lon']}%26latlon%3D%26jkn%3D(COL_06%3A1)%26rad%3D500000%26knsu%3D1000%26exkid%3D%26hour%3D1%26cust%3D%26exarea%3D%26polycol%3D&zdccnt={random.randint(1, 25)}&enc=EUC&encodeflg=0"
          self.get_page(url)
    except:
      raise
    if True:
      self.end = time.time()

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
        bucket.blob(f'/ichsan/ichsan_{self.file_name}.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')

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
    while True:
      try:
        page =self.session.get(url, headers=headers, )    # proxies=self.PROXIES)
        break
      except:
        time.sleep(7)
    store_lists = page.content.decode('euc-jp').split('=')[1].split('\\n')[1:]
    for store in store_lists:
      store = store.split('\\t')
      if store[0] not in self.visited_stores:
        self.visited_stores.append(store[0])
        _store = dict()
        _store['url_store'] = 'https://www.e-map.ne.jp/p/lawson/dtl/' + store[0] + '/'
        try:
          _store['store_name'] = store[6]
        except IndexError:
          continue
       
        _store['address'] = store[7]

        _store['lat'] = ''
        _store['lon'] = ''

        _store['tel_no'] = store[8]
        _store['open_hours'] = '24時間営業' if str(store[10]) == '1' else str(store[11]) + '~' + str(store[12])
        _store['gla'] = ''

        _store['定休日'] = ''  # Regular holiday

        _store['駐車場'] = '有' if store[39] != 0 and store[39] != '' else '無'  # Parking lot Yes ( 有 ) , No ( 無 )

        _store['禁煙・喫煙'] = ''  # [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )

        handling_keys = {'ATM': store[13],
                         'お酒': store[14],
                         'イートイン': store[43],
                         'バリアフリートイレ': store[44],
                         'タバコ': store[15],
                         '薬': store[17],
                         '無印良品': store[51],
                         'まちかど厨房 (店内キッチン)': store[36],
                         'マルチコピー機': store[18],
                         }
        emoney_keys = {'Suica': store[20],
                       'Kitaca': store[21],
                       'ICOCA': store[22],
                       'TOICA': store[23],
                       'nimoca': store[24],
                       'PASMO': store[25],
                       'SUGOCA': store[26],
                       'はやかけん': store[27]}

        _store['取扱'] = ' / '.join([key for key, value in handling_keys.items() if value == 1 or value == '1']) + \
          ' [交通系電子マネー] ' + ', '.join([key for key, value in emoney_keys.items() if value == 1 or value == '1'])   # Handling

        _store['備考'] = ''  # Remarks

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        # print(_store)
        self.content.append(_store)
    self.save_data()
    print(len(self.visited_stores))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-', '-', '-', '-', 'address', 'url_store', 'url_tenant', 'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
if __name__ == '__main__':
  ShoppingConveniLawson(True)

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
