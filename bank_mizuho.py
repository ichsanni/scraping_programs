import json, csv
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


class BankMizuho(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_bank_mizuho.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited = list()

    start = time.time()
    strtime = str(time.time()).replace('.', '')[:13]
    with open('jp-cities-coordinates/jp_coord.csv', 'r', encoding='utf8') as f:
      reader = csv.DictReader(f)
      count = 0
      for x in reader:
        print(count, x['city'])
        count += 1
        self.get_page(x['lat'], x['lng'],strtime)
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

  def get_page(self, lat, lon, strtime):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random}
    lat = float(lat)
    lon = float(lon)
    params = {
      'callback': '',
      'grp': 'one_mizuho',
      'scl': '5',
      'crd': '0',
      'json': '1',
      'vo': 'mbml',
      'entref': '1',
      'dtm': 'wgs',
      'start': '1',
      'pm': '300',
      'nl': lat,
      'el': lon,
      'minnl': lat - 2,
      'minel': lon - 2,
      'maxnl': lat + 2,
      'maxel': lon + 2,
      'poi_status': '1',
      'pp': 'print_start_date,print_end_date',
      'unit': 'bk_kind:schema_id,bk_atm_diff_flag:bk_wheel_rest_flag:bk_diff_flag:bk_osto_flag:bk_handi_flag:bk_bet_flag:bk_wheel_park_flag:bk_aed_flag',
      'bk_net_branch_flag': 'null',
      'bk_kind': '支店,出張所,%店舗外%,外貨両替ショップ,イオン・みずほ共同利用ＡＴＭ',
      '_': str(strtime),
    }

    page = self.session.get("REDACTED URL" params=params, headers=headers)
    _lists_ = re.sub('\((.+)\);', '\g<1>', page.html.html)
    store_lists = json.loads(_lists_)
    try:
      store_lists['mbml']['PoiList']['Poi']
    except KeyError: return
    if isinstance(store_lists['mbml']['PoiList']['Poi'], dict):
      store_lists['mbml']['PoiList']['Poi'] = [store_lists['mbml']['PoiList']['Poi']]
    for store in store_lists['mbml']['PoiList']['Poi']:
      if store['id'] in self.visited: continue
      self.visited.append(store['id'])
      _store = dict()

      _store['url_store'] = f"REDACTED URL"

      _store['store_name'] = store['name']try: 
_store['address'] = store['zip_code'] + store['full_address']
      except: print(store); raise

      _store['lat'] = ''

      _store['lon'] = ''

      try: _store['tel_no'] = store['tel'].replace('<br />', '')
      except KeyError: _store['tel_no'] = ''

      try: _store['open_hours'] = store['bk_atm_time'].replace('<br />', '')
      except KeyError:
        try: _store['open_hours'] = store['sc_open_time'].replace('<br />', '')
        except KeyError: _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
    self.save_data()
    print(len(self.content))

if __name__ == '__main__':
  BankMizuho(True)
