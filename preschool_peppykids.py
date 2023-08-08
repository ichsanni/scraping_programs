import datetime, csv, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from fake_useragent import UserAgent
from requests_html import HTMLSession
import pandas as pd
import re, json
from scraping.scraping import CleaningData
from google.cloud import storage

class PreschoolPeppykids(CleaningData):
  def __init__(self, from_main=False):
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

      end = time.time()
      print("============ ", (end - start) / 60, " minute(s) ============")

      try:
          self.df_clean = self.clean_data(x)
      except:
          raise
      if True:
          if from_main:
              self.df_clean.to_csv('D:/dags/csv/_preschool_peppykids.csv', index=False)
          else:
              # ======== UPLOAD KE BUCKET >>>>>>>>>
              client = storage.Client()
              bucket = client.get_bucket('scrapingteam')
              bucket.blob('/ichsan/_preschool_peppykids.csv').upload_from_string(
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
    print(url)
    headers = {'user-agent': UserAgent().random.strip()}
    page = self.session.get(url, headers=headers)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    store_lists = soup.find_all('td', 'map')
    for store in store_lists:
      link = "REDACTED URL"+ store.find('a')['href'].replace('../', '')
      data = self.get_data(link)
      if data:
        self.content.append(data)

        print(len(self.content))
        time.sleep(0.5)
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

    try: _store['store_name'] = soup.find('div', {'id':'contents'}).find('h1').text.strip()
    except AttributeError: return
_store['address'] = soup.find('dt', text='住所').find_next_sibling('dd').text.strip().replace('\u3000', ' ')


    _store['lat'], _store['lon'] = '', ''

    _store['tel_no'] = soup.find('dt', text='お問い合わせ先').find_next_sibling('dd').text.strip()

    _store['open_hours'] = ''

    _store['gla'] = ''

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    return _store

if __name__ == '__main__':
  PreschoolPeppykids(True)