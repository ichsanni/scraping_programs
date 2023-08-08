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


class UdonWest(CleaningData):
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
        self.df_clean.to_csv('D:/dags/csv/_udon_west.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_udon_west.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_udon_west.csv', index=False)

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

    areas = soup.find('div', {'id':'sideMenu'}).find_all('a')
    for area in areas:
      if 'subarea' in area['href']: continue
      pcnt = 1
      while True:
        link = "REDACTED URL"
        print(link)
        area_page = self.session.get(link, headers=headers)
        area_soup = BeautifulSoup(area_page.html.html, 'html.parser')

        store_counts = self.get_store_lists(area_soup)

        if store_counts == 0: break

        pcnt += 1


  def get_store_lists(self, soup):
    store_lists = soup.find_all('p', 'storeName')
    for store in store_lists:
      link = "REDACTED URL"+ store.find('a')['href']
      self.content.append(self.get_data(link))

      self.save_data()
      print(len(self.content))

    return len(store_lists)


  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup.find('h2').text.replace('\u3000', ' ')
_store['address'] = soup.find('img', {'alt':'所在地'}).parent.find_next_sibling('td').text

    maps_link = "REDACTED URL"+ 
_store['address']

    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup.find('img', {'alt':'TEL'}).parent.find_next_sibling('td').text

    _store['open_hours'] = soup.find('img', {'alt':'営業時間'}).parent.find_next_sibling('td').text.replace('\u3000', ' ')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store


if __name__ == '__main__':
  UdonWest(True)
