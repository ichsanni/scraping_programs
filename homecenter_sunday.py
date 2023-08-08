import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re
from scraping.scraping import CleaningData
from google.cloud import storage


class HomecenterSunday(CleaningData):
  def __init__(self):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()
    url = "REDACTED URL"
    start = time.time()
    self.get_page(url)
    end = time.time()
    print("============ ", (end - start)/60, " minute(s) ============")

    # ===== MULAI KONEKSI KE BIGQUERY
    client = storage.Client()
    bucket = client.get_bucket('scrapingteam')
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
          
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_homecenter_sunday.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
    except:  
      # ======== UPLOAD KE BUCKET >>>>>>>>>
      bucket.blob('/ichsan/_homecenter_sunday.csv').upload_from_string(self.df_clean.to_csv(index=False), 'text/csv')
      raise

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def get_page(self, url):
    page = self.session.get(url)
    soup = BeautifulSoup(page.html.html, 'html.parser')

    link = "REDACTED URL"

    cats = soup.find_all('ul', {'class':'shops'})
    for cat in cats:
      lis = cat.find_all('li')
      for li in lis:
        path = li.find('a').get('href')
        self.content.append(self.get_data(link + path))
    print(len(self.content))

  def get_data(self, url):
    _store = dict()
    page_2 = self.session.get(url)
    soup = BeautifulSoup(page_2.html.html, 'html.parser')
    
    infos = soup.find_all('dd')

    _store['url_store'] = url

    _store['store_name'] = soup.find('h2').text
_store['address'] = infos[0].text.split('\n')[0]

    map = infos[0].find('span').find('a').get('href')
    location = re.findall(r'\d{2,3}\.\d{5,}', map)
    if location:
      maps_link = "REDACTED URL"+ 
_store['address']

      _store['lat'], _store['lon'] = '', ''


    _store['tel_no'] = str(infos[1].text.replace('-', ''))

    try:
      _store['open_hours'] = infos[-2].text.replace('\t', '').replace('\u3000', '').split('\n')[1]
    except IndexError:
      _store['open_hours'] = infos[-2].text.replace('\t', '').replace('\u3000', '')
    
    _store['gla'] = 'Null'

    _store['scrape_date'] = datetime.date.today().strftime('%m/%d/%Y')

    print(_store)
    return _store
        
