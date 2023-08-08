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


class JukuItto(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '_juku_itto.py'.replace('/', '_').replace('.py', '')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.visited = list()

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

    area_lists = soup.find('div', 'shitopSecInner').find_all('a')
    for area in area_lists:
      print(area['href'])
      # if 'itto' not in area['href']: continue
      area_page = self.session.get(area['href'], headers=headers)
      area_soup = BeautifulSoup(area_page.html.html, 'html.parser')

      store_lists = area_soup.find_all('dt', 'pickup_title')
      for store in store_lists:
        link = store.find('a')['href']
        if link in self.visited: continue
        self.visited.append(link)
        link = link if '..' not in link else '/'.join([x for x in area['href'].split('/')[:-3]]) + '/' + link.replace('../', '')
        data = self.get_data(link)
        if data:
          self.content.append(data)

          self.save_data()
          print(len(self.content))

      building_list = area_soup.find_all('div', 'IT')
      for building in building_list:
        link = building.find('a')['href']
        if link in self.visited: continue
        self.visited.append(link)
        link = link if '..' not in link else '/'.join([x for x in area['href'].split('/')[:-3]]) + '/' + link.replace('../', '')
        data = self.get_data(link)
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
    print(url)
    while True:
      try:
        headers = {'user-agent': UserAgent().random}
        page = self.session.get(url, headers=headers)
        soup = BeautifulSoup(page.html.html, 'html.parser')
        soup.find('h2').text.replace(soup.find('span', {'id': 'mainTxt'}).text, '')
        break
      except AttributeError:
        try:
          soup = BeautifulSoup(page.html.html, 'html.parser')
          soup.find('div', {'id':'schoolSec'}).find('h3').text
          break
        except:
          if page.status_code == 404: return
          time.sleep(5)
    _store = dict()

    _store['url_store'] = url

    try: _store['store_name'] = soup.find('h2').text.replace(soup.find('span', {'id':'mainTxt'}).text, '')
    except AttributeError: _store['store_name'] = soup.find('div', {'id':'schoolSec'}).find('h3').text
_store['address'] = soup.find('dt', text=re.compile('住\s+所')).find_next_sibling('dd').text

    _store['lat'] = ''

    _store['lon'] = ''

    try: _store['tel_no'] = soup.find('dt', text='電話番号').find_next_sibling('dd').text
    except: _store['tel_no'] = ''

    try: _store['open_hours'] = soup.find('dt', text='開校情報').find_next_sibling('dd').text
    except: _store['open_hours'] = ''

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    return _store


if __name__ == '__main__':
  JukuItto(True)
