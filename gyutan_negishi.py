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


class GyutanNegishi(CleaningData):
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
        self.df_clean.to_csv('D:/dags/csv/_gyutan_negishi.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_gyutan_negishi.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_gyutan_negishi.csv', index=False)

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

    areas = soup.find_all('table')
    for area in areas:
      store_lists = area.find_all('tr')
      for store in store_lists:
        details = store.find_all('td')
        if len(details) < 4: continue
        _store = dict()

        _store['url_store'] = url

        _store['store_name'] = details[1].find('h4').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', ' ').strip()
_store['address'] = ''.join([x for x in details[1].get_text(separator='\n').split('\n')
                                     if '県' in x or '市' in x or '町' in x or '都' in x
                                     or 'F' in x or 'B' in x]).replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').replace('\u3000', ' ').strip()

        maps_link = "REDACTED URL"+ 
_store['address']

        _store['lat'], _store['lon'] = '', ''

        _store['tel_no'] = ''.join([x for x in details[1].get_text(separator='\n').split('\n') if x.startswith('0')]).replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').replace('\u3000', ' ').strip()

        _store['open_hours'] = details[2].text.split('※')[0].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').replace('\u3000', ' ').strip()

        _store['gla'] = ''

        _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

        # print(_store)
        self.content.append(_store)
    self.save_data()
    print(len(self.content))


if __name__ == '__main__':
  GyutanNegishi(True)
