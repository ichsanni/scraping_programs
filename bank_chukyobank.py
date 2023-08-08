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


class BankChukyobank(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_bank_chukyobank.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited_stores = list()
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
    page = self.session.get(url, headers=headers)
    store_lists = page.content.decode('euc-jp').split('=')[1].split('\\n')[1:]
    for store in store_lists:
      store = store.split('\\t')
      try: store[8]
      except IndexError: continue
      if store[0] not in self.visited_stores and store[8]:
        self.visited_stores.append(store[0])
        link = f"REDACTED URL"
        self.content.append(self.get_data(link, store[6], store[7], store[8]))
        print(len(self.content))
        self.save_data()

  def get_data(self, url, store_name, addr, tel_no):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    while True:
      try:
        self.session = HTMLSession()
        self.session.cookies.clear()
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        _store = dict()

        _store['url_store'] = url

        _store['store_name'] = store_name.replace('\u3000', ' ')address = soup.find('th', text='郵便番号').find_next_sibling('td').text +' '+ addr
        
_store['address'] = address.replace('\n', '').replace('\t', '')

        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = tel_no

        try: _store['open_hours'] = soup.find('th', text='窓口営業時間').find_next_sibling('td').text.replace('\n', '').replace('\t', '')
        except AttributeError: _store['open_hours'] = soup.find('th', text=re.compile('ATM')).find_next_sibling('td').text

        _store['gla'] = ''

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        print(_store)

        break
      except AttributeError:
        raise
        print('attribute error')
        time.sleep(3)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page


if __name__ == '__main__':
  BankChukyobank(True)
