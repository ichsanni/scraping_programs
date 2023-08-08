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


class RamenRamenogawa(CleaningData):
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
        self.df_clean.to_csv('D:/dags/csv/_ramen_ramenogawa.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_ramen_ramenogawa.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_ramen_ramenogawa.csv', index=False)

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

    top = soup.find_all('div', 'h2')[1:]
    bottom = soup.find_all('div', 'h3')
    for store_location, store_details in zip(top, bottom):
      if not store_location.find('span', 'min'): next(store_location)
      store_details = store_details.get_text(separator='\n').split('\n')
      _store = dict()

      _store['url_store'] = url

      try: _store['store_name'] = store_location.find('span', 'got').text
      except AttributeError: _store['store_name'] = '丸にたちばな'
_store['address'] = store_location.find('span', 'min').text.strip()


      _store['lat'], _store['lon'] = '', ''

      _store['tel_no'] = ''.join([x for x in store_details if '電話' in x]).replace('電話', '').replace(':', '')

      _store['open_hours'] = ''.join([x for x in store_details if '営業時間' in x]).replace('営業時間', '').strip()[1:]

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
  RamenRamenogawa(True)
