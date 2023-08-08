from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
import json

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class IdphotoPhotome(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_idphoto_photome.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited = list()

    start = time.time()
    for x in range(0, 48):
      url = "REDACTED URL"
      for y in range(0, 5):
        self.get_page(url, str(x).zfill(2), str(y))
      time.sleep(3)
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

  def get_page(self, url, kencode, myno):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    print(kencode)
    headers = {'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9,id;q=0.8',
                'content-type': 'application/json; charset=UTF-8',
               'user-agent': UserAgent().random}
    data = "{KenID:'" + kencode + "', AddrPart:'', SearchTxt:'', RBSearchType:'1', MyNo:'" + myno + "'}"
    # 0,2: 8582
    page = self.session.post("REDACTED URL" headers=headers, data=data).json()
    store_lists = json.loads(page['d'])
    for store in store_lists:
      if (store['Address'], store['LocNmKanji']) in self.visited: continue
      self.visited.append((store['Address'], store['LocNmKanji']))
      _store = dict()

      _store['url_store'] = "REDACTED URL"

      _store['store_name'] = store['LocNmKanji']
_store['address'] = store['Address']

      _store['lat'] = ''

      _store['lon'] = ''

      _store['tel_no'] = store['TelNo']

      _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)
    print(len(self.content))
    self.save_data()

if __name__ == '__main__':
  IdphotoPhotome(True)
