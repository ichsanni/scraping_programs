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


class GmsModi(CleaningData):
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
    finally:
      if from_main:
        self.df_clean.to_csv('D:/dags/csv/_gms_modi.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_gms_modi.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_gms_modi.csv', index=False)

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
    visited = list()

    for store in soup.select('h4.store-name a'):
      store_name = store.get_text(strip=True)
      if re.search('モディ', store_name):
        url_store = f"REDACTED URL"
        if url_store in visited: continue
        visited.append(url_store)
        self.content.append(self.get_data(url_store))

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
    soup = BeautifulSoup(page.html.html, 'xml')
    _store = dict()

    _store['url_store'] = url.replace('/facility.xml', '').replace('contents/facilityinfo/', '')

    _store['store_name'] = soup.find('facility_name').text
_store['address'] = BeautifulSoup(soup.find('title', text='住所').find_next_sibling('text').text, 'html.parser').text.replace('\n', '').replace('\r', '')

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

    _store['tel_no'] = BeautifulSoup(soup.find('title', text='電話番号').find_next_sibling('text').text, 'html.parser').text.replace('\n', '').replace('\r', '').split('受付時間')[0]

    _store['open_hours'] = BeautifulSoup(soup.find('title', text='通常営業時間').find_next_sibling('text').text, 'html.parser').text.replace('\n', '').replace('\r', '')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page.html.html


if __name__ == '__main__':
  GmsModi(True)
