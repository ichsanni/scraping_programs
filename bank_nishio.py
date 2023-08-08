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


class BankNishio(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_bank_nishio.py'.replace('/', '_').replace('.py', '')
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
    headers = {'user-agent': UserAgent().random}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    areas = soup.find_all('p', 'tenpo_title', text='店舗')
    for area in areas:
      store_lists = area.find_next_sibling('div', 'tenpolist').find_all('a')
      for store in store_lists:
        link = "REDACTED URL"+ store['href']
        self.content.append(self.get_data(link))

        self.save_data()
        print(len(self.content))

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('h2').text
_store['address'] = soup.find('th', text='住所').find_next_sibling('td').text

    maps_link = "REDACTED URL"+ 
_store['address']
    maps = self.get_map_data(maps_link)
    coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps)
    # print(coords)
    if coords:
      _store['lat'] = coords[0].split(',')[0].replace('@', '')
      _store['lon'] = coords[0].split(',')[1]
    else:
      _store['lat'] = ''
      _store['lon'] = ''

    try: _store['tel_no'] = soup.find('th', text='TEL').find_next_sibling('td').text
    except AttributeError: _store['tel_no'] = ''

    try: _store['open_hours'] = soup.find('th', text='平日').find_next_sibling('td').text
    except AttributeError: _store['open_hours'] = ''

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page.html.html


if __name__ == '__main__':
  BankNishio(True)
