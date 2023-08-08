import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class SuperBelc(CleaningData):
  def __init__(self, from_main):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    self.file_name = '_super_belc'

    url = "REDACTED URL"
    start = time.time()
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start)/60, " minute(s) ============")

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

  def get_page(self, url):
    '''
    Visit the link given from the gsheet,
    see if there's data there.
    If yes, scrape immediately.
    If no, call get_data(url) and visit individual pages.
    '''
    page = self.session.get(url)
    soup = BeautifulSoup(page.html.html, 'html.parser')
    domain = "REDACTED URL"

    rows = soup.find_all('a', 'm-btn-detail')
    for row in rows:
      path = domain + row['href'] if 'belc' not in row['href'] else row['href']
      self.content.append(self.get_data(path))
      print(len(self.content))

  def get_data(self, url):
    '''
    Visit individual page,
    see if you can scrape map latitude and longitude.
    If no, visit map individuually by calling get_map_data(url)
    '''
    print(url)
    page_2 = self.session.get(url)
    soup_2 = BeautifulSoup(page_2.html.html, 'html.parser')

    _store = dict()

    _store['url_store'] = url

    _store['store_name'] = soup_2.find('h1', {'class':'l-mv__hdg'}).text.replace('\n', '')
_store['address'] = soup_2.find('div', text=re.compile('住所')).parent.find_next('dd').text.replace('\n', '').replace(' ', '')


    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup_2.find('div', text=re.compile('電話番号')).parent.find_next('dd').text.replace('\n', '').replace(' ', '')

    _store['open_hours'] = soup_2.find('div', text=re.compile('営業時間')).parent.find_next('dd').text.replace('\n', '').replace(' ', '')
    
    _store['gla'] = 'Null'

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')
    
    # print(_store)
    return _store

  def get_map_data(self, url):
    headers = {'referer':"REDACTED URL"Not A;Brand";v="99", "Microsoft Edge";v="92"', 'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.84'}
    page = self.session.get(url, headers=headers, allow_redirects=True)

    return page

if __name__ == '__main__':
  SuperBelc(True)
