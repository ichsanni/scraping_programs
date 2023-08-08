import json
import random

import csv

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


class BankMitsui(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_bank_mitsui.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited = list()

    start = time.time()
    store_count = 400

    # with open('jp-cities-coordinates/koordinat_20km.csv', 'r', encoding='utf8') as f:
    #   reader = csv.DictReader(f)
    reader = [[31.840357395270217, 131.18746859979515], [35.74663134668644, 139.5810232655242], [44.3710916919436, 142.92086700686144]]
    for city in reader:
      lat = city[0] # float(city['lat'])
      lon = city[1] # float(city['lon'])
      # print('--', city['index'], city['lat'], city['lon'])
      # url = f"REDACTED URL"+\
      #       f"target=http%3A%2F%2Fvstorenaviweb.vmc.zdc.local%2Fcgi%2Fstore_nearsearch.cgi%3F" +\
      #       f"from=js&intid=EmapMlSsQ&lang=en&cid=smbcbank&opt=smbcbank&" +\
      #       f"pos=1&cnt={store_count}&knsu={store_count}&" +\
      #       f"lat={lat}&lon={lon}&nelat={lat + 2}&nelon={lon + 2}&swlat={lat - 2}&swlon={lon - 2}" +\
      #       f"&filter=(COL_77%3A1)&ewdist=0&sndist=0&exkid=&pflg=1&" +\
      #       f"cols=&zdccnt={random.randint(1, 25)}&enc=EUC&encodeflg=0"
      url = f"REDACTED URL"\
            f"target=http%3A%2F%2Fvstorenaviweb.vmc.zdc.local%2Fcgi%2Fstore_nearsearch.cgi%3Ffrom%3Djs%26intid%3D" \
            f"EmapMlSsQ%26lang%3Den%26cid%3Dsmbcbank%26opt%3Dsmbcbank%26pos%3D1%26cnt%3D{store_count}%26knsu%3D{store_count}%26" \
            f"lat%3D{lat}%26lon%3D{lon}%26filter%3D(COL_77%3A0)%26ewdist%3D5000000%26sndist%3D5000000%26" \
            f"exkid%3D%26pflg%3D1%26cols%3D&zdccnt={random.randint(1, 25)}&enc=UTF8&encodeflg=0"
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
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    while True:
      try:
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        break
      except: time.sleep(3)
    json_data = re.sub(".+\] = '(.+)'.+", "\g<1>", page.html.html.replace("\\", ''))
    try: store_lists = json.loads(json_data)
    except: print(json_data); raise
    try: store_lists['store_list']
    except KeyError: return 0
    for store in store_lists['store_list']:
      if store['store_id'] in self.visited: continue
      self.visited.append(store['store_id'])
      _store = dict()

      _store['url_store'] = "REDACTED URL"+ store['store_id']

      _store['store_name'] = [x['text'] for x in store['content'] if x['col'] == 'NAME'][0]
_store['address'] = [x['text'] for x in store['content'] if x['col'] == 'ADDR'][0]

      _store['lat'] = ''

      _store['lon'] = ''

      _store['tel_no'] = [x['text'] for x in store['content'] if x['name'] == '電話番号'][0]

      _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
    print(len(self.content))
    self.save_data()


if __name__ == '__main__':
  BankMitsui(True)
