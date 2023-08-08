from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass

# 【Cilandak Barat】
class IzakayaWatami(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_izakaya_watami.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = ""
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    x = pd.DataFrame(self.content)

    # CLEANING 1: PERIKSA KOORDINAT
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
    finally:
      if from_main:
        self.df_clean.to_csv(f'{self.file_name}.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    headers = {'user-agent': UserAgent().random,
               'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',}
    data = {
      'keyword': '["津軽の酒処"]',
      'brand': '[null]',
    }

    store_lists = self.session.post("REDACTED URL" headers=headers, data=data).json()
    for store in store_lists['data']:
      _store = dict()
      _store['store_name'] = store['shop_name']
_store['address'] = f"〒{store['zip1']}-{store['zip2']}{store['prefname']}{store['town']}{store['addr']} {store['build']}"
      _store['url_store'] = f"REDACTED URL"
      _store['url_tenant'] = ''
      _store['営業時間'] = store['opentime'][0]# Open hours / Business Hours
      _store['lat'] = store['latitube']
      _store['lon'] = store['longitube']
      _store['tel_no'] = store['telno']
      _store['gla'] = ''
      _store['定休日'] = '' # Regular holiday
      _store['駐車場'] = '' # Parking lot Yes ( 有 ) , No ( 無 )
      _store['禁煙・喫煙'] = ''# [Non-smoking/Smoking] Yes ( 有 ) , No ( 無 )
      _store['取扱'] = '' # Handling
      _store['備考'] = f"{store['seatcount']}席 | {' | '.join(store['caracter_label_list'])}" # Remarks
      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      print(_store)
      self.content.append(_store)
      print(len(self.content))

if __name__ == '__main__':
  IzakayaWatami(True)


# PINDAHKAN INI KE ATAS JIKA MENGGUNAKAN RENDER
# from requests_html import AsyncHTMLSession
# import pyppeteer, asyncio
# class AsyncHTMLSessionFixed(AsyncHTMLSession):
#   def __init__(self, **kwargs):
#     super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
#     self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])
#   @property
#   async def browser(self):
#     if not hasattr(self, "_browser"):
#       self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not (self.verify), headless=True, handleSIGINT=False,
#                                              handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)
#     return self._browser

# TAMBAHKAN LINE INI UNTUK DEF YANG MENGGUNAKAN RENDER
# loop = asyncio.new_event_loop()
# loop.run_until_complete()