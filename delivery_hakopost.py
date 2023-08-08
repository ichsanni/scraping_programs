import csv
import random
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


class DeliveryHakopost(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_delivery_hakopost.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited_stores = list()
    start = time.time()
    with open('jp-cities-coordinates/prefektur.csv', 'r', encoding='utf8') as f:
      reader = csv.DictReader(f)
      count = 0
      for x in reader:
        print(count, x['nama'])
        count += 1
        url = f"REDACTED URL"\
              f"cid%3Djppost15%26opt%3Djppost21%26pos%3D1%26cnt%3D1000%26knsu%3D1000%26enc%3DEUC%26" \
              f"lat%3D{x['lat']}%26lon%3D{x['lon']}%26latlon%3D%26" \
              f"jkn%3D(COL_02%3A1100)%20AND%20(COL_02%3A1100)%20AND%20(NOT%20COL_07%3A1)%26" \
              f"rad%3D5000000%26exkid%3D%26hour%3D1%26cust%3D%26exarea%3D%26polycol%3D&zdccnt={random.randint(1,99)}&enc=EUC&encodeflg=0"
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
    if True:
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
    headers = {'user-agent': UserAgent().random.strip()}
    while True:
      try:
        page = self.session.get(url, headers=headers)
        break
      except:
        time.sleep(5)
    store_lists = page.content.decode('euc-jp').split('=')[1].split('\\n')[1:]
    for store in store_lists:
      store = store.split('\\t')
      if store[0] not in self.visited_stores:
        self.visited_stores.append(store[0])
        _store = dict()
        _store['url_store'] = "REDACTED URL"
        try:
          _store['store_name'] = store[6]
        except IndexError:
          continue
_store['address'] = store[-4] + store[8]

        # maps_link = "REDACTED URL"+ 
_store['address']
        # maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        # coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
        # # print(coords)
        # if coords:
        #   _store['lat'] = coords[0].split(',')[0].replace('@', '')
        #   _store['lon'] = coords[0].split(',')[1]
        # else:
        #   try: location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   except:
        #     time.sleep(5)
        #     maps_link = "REDACTED URL"+ 
_store['address']
        #     maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        #     location = maps.html.find('meta[property="og:image"]', first=True).attrs['content']
        #   try:
        #       lat_lon = location.split('&markers=')[1].split('%7C')[0].split('%2C')
        #       _store['lat'], _store['lon'] = lat_lon[0], lat_lon[1]
        #   except:
        #       lat_lon = location.split('center=')[1].split('&zoom')[0].split('%2C')
        _store['lat'], _store['lon'] = '',''

        _store['tel_no'] = ''
        _store['open_hours'] = store[19]
        _store['gla'] = ''
        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
        self.content.append(_store)
    self.save_data()
    print(len(self.visited_stores))


if __name__ == '__main__':
  DeliveryHakopost(True)
