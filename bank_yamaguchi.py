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


class BankYamaguchi(CleaningData):
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
    if True:
      if from_main:
        self.df_clean.to_csv('D:/dags/hasil/_bank_yamaguchi.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_bank_yamaguchi.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_bank_yamaguchi.csv', index=False)

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

    cities = soup.find_all('div', 'l_content')
    for city in cities:
      areas = city.find_all('a')
      for area in areas:
        if 'foreign' in area['href']: continue
        link = "REDACTED URL"+ area['href']
        area_page = self.session.get(link, headers=headers)
        area_soup = BeautifulSoup(area_page.html.html, 'html.parser')

        store_lists = area_soup.find_all('table', 'table')
        for store in store_lists:
          _store = dict()

          _store['url_store'] = area_page.url

          _store['store_name'] = store.find('th', 'table_tit').text.split('（')[0]
_store['address'] = store.find('div', 'e_address').text.replace('\n', '').replace('\t', '')


          _store['lat'], _store['lon'] = '', ''

          try: _store['tel_no'] = store.find('td', text='電話番号').find_next_sibling('td').text.replace('\n', '') \
                .replace('\t', '').replace('\r', '').replace('\u3000', ' ')
          except AttributeError: _store['tel_no'] = ''

          try: _store['open_hours'] = store.find('td', text='窓口営業時間').find_next_sibling('td').text.replace('\n', '') \
                .replace('\t', '').replace('\r', '').replace('\u3000', ' ')
          except AttributeError:
            try: _store['open_hours'] = store.find('td', text='ATM稼働時間').find_next_sibling('td').text.replace('\n', '') \
                .replace('\t', '').replace('\r', '').replace('\u3000', ' ')
            except AttributeError: _store['open_hours'] = ''

          _store['gla'] = ''

          _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

          print(_store)
          self.content.append(_store)

          self.save_data()
          print(len(self.content))

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page


if __name__ == '__main__':
  BankYamaguchi(True)
