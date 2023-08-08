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


class BankYamanashi(CleaningData):
  def __init__(self, from_main=False):
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    page_count = 1
    while True:
      url = f"REDACTED URL"
      try: self.get_page(url)
      except StopIteration: break
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
        self.df_clean.to_csv('D:/dags/csv/_bank_yamanashi.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_bank_yamanashi.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_bank_yamanashi.csv', index=False)

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

    store_lists = soup.find_all('div', 'list-block-name')
    if len(store_lists) == 0: raise StopIteration
    for store in store_lists:
      link = "REDACTED URL"+ store.find('a')['href']
      self.content.append(self.get_data(link, store_name=store.find('a').text))
      print(len(self.content))
    self.save_data()

  def get_data(self, url, store_name):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individually by calling get_map_data(url)
    '''
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    time.sleep(1)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = store_name
_store['address'] = soup.find('th', text='住所').find_next_sibling('td').text.replace('\n', '').replace('\r',
                                                                                                          '').replace(
      '\t', '').replace('\u3000', ' ').strip()

    try:
      _store['tel_no'] = soup.find('th', text='TEL').find_next_sibling('td').text.replace('\n', '').replace('\r',
                                                                                                             '').replace(
        '\t', '').replace('\u3000', ' ')
    except AttributeError:
      _store['tel_no'] = ''

    try:
      _store['open_hours'] = soup.find('h2', text='店舗営業時間').parent.find_next_sibling('tbody').text.replace('\n', '').replace('\r',
                                                                                                               '').replace(
        '\t', '').replace('\u3000', ' ')
    except AttributeError:
      _store['open_hours'] = soup.find('h2', text=re.compile('ATM営業時間')).parent.find_next_sibling('tbody').text.replace('\n', '').replace('\r',
                                                                                                                '').replace(
        '\t', '').replace('\u3000', ' ')

    _store['lat'] = ''
    _store['lon'] = ''
    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    return _store



if __name__ == '__main__':
  BankYamanashi(True)
