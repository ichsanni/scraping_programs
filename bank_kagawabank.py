from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData

try: from google.cloud import storage
except ModuleNotFoundError: pass


class BankKagawabank(CleaningData):
  def __init__(self, from_main=False):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
    self.get_page(url, from_main)
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
        self.df_clean.to_csv('D:/dags/csv/_bank_kagawabank.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_bank_kagawabank.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    df = pd.DataFrame(self.content)
    df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                             '-', '-', '-', 'address', 'url_store', 'url_tenant',
                             'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
    df.to_csv('D:/dags/csv/_bank_kagawabank.csv', index=False)

  def get_page(self, url, from_main):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    headers = {'user-agent': UserAgent().random.strip()}
    data = {
      'Item': 'Search',
      'selArea': '',
      'txtTenpomei': '',
      'txtJyusyo': '',
    }
    page = self.session.post(url, headers=headers, data=data)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    store_lists = soup.select('table.shop__table tbody tr a')
    for store in store_lists:
      link = "REDACTED URL"+ store['href']
      self.content.append(self.get_data(link))

      if from_main: self.save_data()
      print(len(self.content))

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

    _store['store_name'] = soup.find('h2').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000',
                                                                                                              ' ')
_store['address'] = soup.find('th', text='住所').find_next_sibling('td').text.replace('\n', '').replace('\t',
                                                                                                          '').replace(
      '\r', '').replace('\u3000', ' ')

    maps_link = "REDACTED URL"+ 
_store['address']

    _store['lat'], _store['lon'] = '', ''

    try:
      _store['tel_no'] = soup.find('th', text='電話番号').find_next_sibling('td').text.replace('\n', '').replace('\t',
                                                                                                             '').replace(
        '\r', '').replace('\u3000', ' ')
    except AttributeError:
      _store['tel_no'] = ''

    _store['open_hours'] = soup.find('th', text=re.compile('時間')).find_next_sibling('td').text.replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '').replace(
      '\r', '').replace('\u3000', ' ')

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    return _store


if __name__ == '__main__':
  BankKagawabank(True)

