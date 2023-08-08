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


class GambleTakarakuji(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
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
        self.df_clean.to_csv('C:/dags/src-flask/_gamble_takarakuji.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_gamble_takarakuji.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('C:/dags/src-flask/_gamble_takarakuji.csv', index=False)

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random,}
    page_count = 0
    while True:
      data = {
        'keyword': '',
        'searchType': 'keyword',
        'pageno': str(page_count),
        'jumboSales': '1',
        'numbersSales': '1',
        'atm': '1',
        'point': '1',
        'timeout': '3000',
        '_csrf': 'b2b987a3-1740-4e04-942f-328ce4bb5ea6',
        '__fromScreenId': 'SC_WMF_SP_101',
        '__ope': '店舗一覧情報取得',
      }
      page_count += 1
      store_lists = self.session.post(url, headers=headers, data=data).json()
      if store_lists['pager']['totalCount'] == len(self.content): break
      for store in store_lists['shops']:
        _store = dict()

        _store['url_store'] = f"REDACTED URL"

        _store['store_name'] = store['name']
_store['address'] = store['address']

        _store['lat'] = store['latitude']

        _store['lon'] = store['longitude']

        _store['tel_no'] = ''

        _store['open_hours'] = ''

        _store['gla'] = ''

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        # print(_store)
        self.content.append(_store)
      print(len(self.content))
      self.save_data()
      time.sleep(0.5)


if __name__ == '__main__':
  GambleTakarakuji(True)
