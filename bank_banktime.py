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

class BankBanktime(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_bank_banktime.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.asession = None
    self.content = list()

    start = time.time()
    self.get_page()
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

  def get_page(self):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    print('starto')
    headers = {'user-agent': UserAgent().random,
               'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',}
    data = {
        'nelat': '55.0',
        'swlat': '20.0',
        'nelng': '150.0',
        'swlng': '120.0',
        'shopids': '',
    }
    page = self.session.get("REDACTED URL" headers=headers, data=data).json()
    for store in page['features']:
      if store['properties'][3] == 6:
        data = self.get_data(store['properties'][0])
        if data:
          self.content.append(data)
          self.save_data()
          print(len(self.content))

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    headers = {'user-agent': UserAgent().random}
    url = f"REDACTED URL"
    print(url)
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.content.decode('utf8'), 'html.parser')
    _store = dict()

    _store['url_store'] = url

    try: _store['store_name'] = soup.find('span', {'id':'de_name'}).text
    except AttributeError: return
_store['address'] = soup.find('span', text=re.compile('郵便番号')).parent.find_next_sibling('td').text + soup.find('span', text=re.compile('住所')).parent.find_next_sibling('td').text

    _store['lat'] = ''

    _store['lon'] = ''

    _store['tel_no'] = ''

    _store['open_hours'] = soup.find('span', text=re.compile('営業時間')).parent.find_next_sibling('td').text

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    return _store


if __name__ == '__main__':
  BankBanktime(True)
