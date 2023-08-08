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


class BankHyakugo(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    page_count = 1
    while True:
      url = f"REDACTED URL"
      print(url)
      data = self.get_page(url)
      if data == 0: break
      page_count += 1

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
        self.df_clean.to_csv('D:/dags/csv/_bank_hyakugo.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_bank_hyakugo.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_bank_hyakugo.csv', index=False)

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    store_lists = soup.find_all('dl', 'MapiAddr')
    for store in store_lists:
      icons = store.parent.parent.find_next('tr').find('td', 'MapiIcon').find_all('img')
      is_atm = True
      for icon in icons:
        if icon['alt'] in ['パーソナルプラザ', 'ほけんの相談窓口', 'インターネット支店']:
          is_atm = False
      if not is_atm: continue
      _store = dict()

      _store['url_store'] = "REDACTED URL"+ store.find('a')['href']

      _store['store_name'] = store.find('span', 'MapiShopName').text
_store['address'] = store.find('dd', 'MapiInfoAddr').text.replace('\xa0', '')

      _store['lat'], _store['lon'] = '',''

      store_page = self.session.get(_store['url_store'], headers=headers)
      print(store_page.url)
      store_soup = BeautifulSoup(store_page.html.html, 'html.parser')
      try:
        _store['tel_no'] = store_soup.find('th', text='TEL').find_next('td').text
      except AttributeError:
        _store['tel_no'] = ''

      try: unrelated_info = store.find('li', 'MapiOpenTimeListInfo').text
      except: unrelated_info = ''
      try: _store['open_hours'] = store.find('ul', 'MapiOpenTimeList').text.replace(unrelated_info, '').split('TEL：')[0].split('TEL:')[0].replace('\n', '').replace('\t', '')\
        .replace('\r', '').replace('\u3000', '').replace('\xa0', '')
      except AttributeError: _store['open_hours'] = ''

      _store['gla'] = ''

      _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

      # print(_store)
      self.content.append(_store)

      print(len(self.content))
    self.save_data()

    return len(store_lists)

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page


if __name__ == '__main__':
  BankHyakugo(True)
