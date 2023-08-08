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


class LanguageNova(CleaningData):
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
        self.df_clean.to_csv('D:/dags/csv/_language_nova.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob('/ichsan/_language_nova.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def save_data(self):
    if self.from_main:
      df = pd.DataFrame(self.content)
      df = df.reindex(columns=['store_name', '-', '-', '-', '-',
                               '-', '-', '-', 'address', 'url_store', 'url_tenant',
                               'open_hours', 'lat', 'lon', 'tel_no', 'gla', 'scrape_date'])
      df.to_csv('D:/dags/csv/_language_nova.csv', index=False)

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

    area_lists = soup.find('div', {'id':'area-top'}).find_all('a')
    for area in area_lists:
      link = url + area['href']
      self.get_city_lists(link)


  def get_city_lists(self, link):
    print('get city list: ' + link)
    headers = {'user-agent': UserAgent().random.strip()}
    area_page = self.session.get(link, headers=headers)
    area_soup = BeautifulSoup(area_page.html.html, 'html.parser')

    store_lists = area_soup.find_all('dd')
    if store_lists:
      self.get_store_lists(link, store_lists)
    else:
      city_lists = area_soup.select('div.area-list4 a')
      for city in city_lists:
        self.get_city_lists('/'.join(area_page.url.split('/')[:-1]) +'/'+ city['href'])

  def get_store_lists(self, url, store_lists):
    for store in store_lists:
      if "REDACTED URL"in store.find('a')['href']: continue
      store_link = '/'.join(url.split('/')[:-1]) + '/' + store.find('a')['href'] if 'schools' not in store.find('a')['href'] \
        else "REDACTED URL"+ store.find('a')['href']

      self.content.append(self.get_data(store_link))

      self.save_data()
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

    _store['store_name'] = soup.find('h1').find('span', 'ttlTxt').text.split('｜')[0]try: 
_store['address'] = re.findall('l_add.*"(.*)";', page.html.html)[0]
    except IndexError:
      try: 
_store['address'] = soup.find('dt', text=re.compile('住所')).find_next_sibling('dd').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
      except AttributeError: 
_store['address'] = soup.find('dd', 'address').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')

    maps_link = soup.find_all('iframe')[1]['src']

    _store['lat'], _store['lon'] = '', ''

    try: _store['tel_no'] = soup.find('dd', {'id':'fs30'}).text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')
    except AttributeError: _store['tel_no'] = soup.find('dt', text='電話番号').find_next_sibling('dd').text.replace('\n', '').replace('\t', '').replace('\r', '').replace('\u3000', '')

    try:
      open_hours_num = re.findall('inKaikou.*"(.*)";', page.html.html)[0].split('/')
      open_hours_day = soup.select('table[summary*="time"] th')
      open_hours = [''.join([x.text,y]) for x,y in zip(open_hours_day, open_hours_num)]
      _store['open_hours'] = ' '.join(open_hours)
    except IndexError: _store['open_hours'] = ''

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer': "REDACTED URL" 'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page


if __name__ == '__main__':
  LanguageNova(True)
