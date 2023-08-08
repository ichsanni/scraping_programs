import requests
from fake_useragent import UserAgent
import datetime, time
from bs4 import BeautifulSoup
# IMPORT PENTING
from requests_html import HTMLSession
import pandas as pd
import re

from urllib3.exceptions import IncompleteRead

from scraping.scraping import CleaningData

try:
  from google.cloud import storage
except ModuleNotFoundError:
  pass


class SuperIzumiya(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '/super/izumiya'.replace('/', '_')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content_izumiya = list()
    self.content_qanat = list()

    start = time.time()
    self.extract()
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    izumiya = pd.DataFrame(self.content_izumiya)
    izumiya.columns = [y.lower() for y in izumiya.columns]

    # CLEANING 1: PERIKSA KOORDINAT
    izumiya['lat'] = pd.to_numeric(izumiya['lat'], errors='coerce')
    izumiya['lon'] = pd.to_numeric(izumiya['lon'], errors='coerce')
    izumiya.loc[izumiya[(izumiya['lat'] < 20) | (izumiya['lat'] > 50)].index, 'lat'] = pd.NA
    izumiya.loc[izumiya[(izumiya['lat'] < 20) | (izumiya['lat'] > 50)].index, 'lon'] = pd.NA

    izumiya.loc[izumiya[(izumiya['lon'] < 121) | (izumiya['lon'] > 154)].index, 'lat'] = pd.NA
    izumiya.loc[izumiya[(izumiya['lon'] < 121) | (izumiya['lon'] > 154)].index, 'lon'] = pd.NA
    izumiya['url_tenant'] = None

    try:
      self.df_clean = self.clean_data(izumiya)
    except:
      raise
    if True:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/_super_izumiya.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/_super_izumiya.csv').upload_from_string(
          self.df_clean.to_csv(index=False), 'text/csv')

    qanat = pd.DataFrame(self.content_qanat)
    qanat.columns = [y.lower() for y in qanat.columns]

    # CLEANING 1: PERIKSA KOORDINAT
    qanat['lat'] = pd.to_numeric(qanat['lat'], errors='coerce')
    qanat['lon'] = pd.to_numeric(qanat['lon'], errors='coerce')
    qanat.loc[qanat[(qanat['lat'] < 20) | (qanat['lat'] > 50)].index, 'lat'] = pd.NA
    qanat.loc[qanat[(qanat['lat'] < 20) | (qanat['lat'] > 50)].index, 'lon'] = pd.NA

    qanat.loc[qanat[(qanat['lon'] < 121) | (qanat['lon'] > 154)].index, 'lat'] = pd.NA
    qanat.loc[qanat[(qanat['lon'] < 121) | (qanat['lon'] > 154)].index, 'lon'] = pd.NA
    qanat['url_tenant'] = None

    try:
      self.df_clean = self.clean_data(qanat)
    except:
      raise
    if True:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/_super_qanat.csv', index=False)
      else:
        # ======== UPLOAD KE BUCKET >>>>>>>>>
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/_super_qanat.csv').upload_from_string(
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

  def del_whitespace(self, text_content):
    text_content = " ".join(text_content) if text_content else ""
    return text_content

  def getResponse(self, url):
    try:
      r = self.session.get(url)
    except IncompleteRead:
      return
    except requests.exceptions.ChunkedEncodingError:
      return
    except ConnectionError:
      # time.sleep(60)
      return

    return (BeautifulSoup(r.html.html, 'html.parser'))

  def doubleReqCoordinate(self, mapsLink):
    # session = requests.Session() #request session untuk maps dan jangan lupa import session
    respmap = self.session.head(mapsLink,
                           allow_redirects=True)  # mapslink di pakai redirect agar bisa mengambil url paling akhir
    return (re.search('[@|=](.[0-9]?[0-9]\.[0-9]*,.[0-9]?[0-9]*\.[0-9]*)', respmap.url, re.DOTALL))

  def extract(self):
     url_main = "REDACTED URL"
     soup = self.getResponse(url_main)
     detail_list = {}

     i = 0
     get_shops = soup.find_all('div', {'class': 'shop-category'})
     for shop in get_shops:
       find_qanat = shop.find('p', {'class': 'shop-panel_trigger'}).text
       print("==============================================")
       print("カナート（株）" == find_qanat)
       print("==============================================")
       cards = shop.find_all('li')
       # cards_store = soup.find_all('h3')
       # cards_details = soup.find_all('td',{'class':'t_add'})
       session = requests.Session()
       # print(cards)
       for card in cards:
         # inCards = card.find_all('p',{'class':'shop-ttl02'})
         inCards = card.find_all('p', {'class': 'shop-ttl02'})
         # print(inCards)

         for inCard in inCards:
           list_data = {}
           try:
             i = i + 1
             print(i)
             try:
               list_data['store_name'] = inCard.find('span', {'class': 'inline'}).text
             except:
               list_data['store_name'] = inCard.text

             if "カナート（株）" == find_qanat:
               # ==============Fixed data====================
               list_data['-'] = 'カナート'
               list_data['-'] = 'SM'
               list_data['-'] = '/super/qanat'
               list_data['-'] = 'QANAT'
               list_data['-'] = 'super'
               list_data['-'] = 'ショッピング'
               list_data['-'] = 'スーパー'
             else:
               list_data['-'] = 'イズミヤ'
               list_data['-'] = 'SuC'
               list_data['-'] = '/super/izumiya'
               list_data['-'] = 'Izumiya'
               list_data['-'] = 'super'
               list_data['-'] = 'ショッピング'
               list_data['-'] = 'スーパー'
               # ============================================

             try:
               if "カナート（株）" != find_qanat:  # kalo izumiya
                 store_url = inCard.find('a').get('href')
                 soup_url = self.getResponse(store_url)
                 address = soup_url.find('div', {'class': 'shop-detail_info__txt'}).find('p').stripped_strings
                 list_data['address'] = self.del_whitespace(address)
                 list_data['url_store'] = store_url
                 list_data['url_tenant'] = ''  # null
                 open_hours = soup_url.find('div', {'class': 'shop-detail_info__txt'}).find_next('div', {
                   'class': 'shop-detail_info__txt'}) \
                   .find_next('div', {'class': 'shop-detail_info__txt'}).find('p').stripped_strings
                 list_data['open_hours'] = self.del_whitespace(open_hours)
                 maps_link = "REDACTED URL"+ list_data['address']
                 maps = self.session.get(maps_link, allow_redirects=True)
                 coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
                 # print(coords)
                 if coords:
                   list_data['lat'] = coords[0].split(',')[0].replace('@', '')
                   list_data['lon'] = coords[0].split(',')[1]
                 else:
                   list_data['lat'] = ''
                   list_data['lon'] = ''
                 Tel_no = soup_url.find('div', {'class': 'shop-detail_info__txt'}).find_next('div', {
                   'class': 'shop-detail_info__txt'}) \
                   .find_next('div', {'class': 'shop-detail_info__txt'}).find_next('div', {
                   'class': 'shop-detail_info__txt'}).find('p').stripped_strings
                 list_data['Tel_no'] = self.del_whitespace(Tel_no)
                 list_data['GLA'] = ''
                 list_data['scrape_date'] = datetime.datetime.today().strftime("%m/%d/%Y")
               else:  # kalo qanat
                 store_url = url_main
                 address = card.find('dl', {'class': 'shop-address--cnt'}).find('dd').text.replace('\r', '').replace('\t',
                                                                                                                     '').split(
                   '\n')
                 list_data['address'] = self.del_whitespace(address[0])
                 list_data['url_store'] = store_url
                 list_data['url_tenant'] = ''  # null
                 open_hours = address[1:]
                 list_data['open_hours'] = self.del_whitespace(open_hours)
                 maps_link = "REDACTED URL"+ list_data['address']
                 maps = self.session.get(maps_link, allow_redirects=True)
                 coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
                 # print(coords)
                 if coords:
                   list_data['lat'] = coords[0].split(',')[0].replace('@', '')
                   list_data['lon'] = coords[0].split(',')[1]
                 else:
                   list_data['lat'] = ''
                   list_data['lon'] = ''
                 Tel_no = card.find_all('dl', {'class': 'shop-address'})[1].find('dd').stripped_strings
                 list_data['Tel_no'] = self.del_whitespace(Tel_no)
                 list_data['GLA'] = ''
                 list_data['scrape_date'] = datetime.datetime.today().strftime("%m/%d/%Y")
             except:
               raise

             print(list_data)
             print("===========================")
             # master_list.append(list_data)
             # saveToCsv()
           except:
             list_data['store_name'] = inCard.text
             if "カナート（株）" == find_qanat:  # kalo qanat
               # ==============Fixed data====================
               list_data['-'] = 'カナート'
               list_data['-'] = 'SM'
               list_data['-'] = '/super/qanat'
               list_data['-'] = 'QANAT'
               list_data['-'] = 'super'
               list_data['-'] = 'ショッピング'
               list_data['-'] = 'スーパー'
             else:  # kalo izumiya
               list_data['-'] = 'イズミヤ'
               list_data['-'] = 'SuC'
               list_data['-'] = '/super/izumiya'
               list_data['-'] = 'Izumiya'
               list_data['-'] = 'super'
               list_data['-'] = 'ショッピング'
               list_data['-'] = 'スーパー'
               # ============================================
             list_data['address'] = card.find('dd').text.split('\n', 1)[0]
             list_data['url_store'] = url_main
             list_data['url_tenant'] = ''  # null
             list_data['open_hours'] = ''
             maps_link = "REDACTED URL"+ list_data['address']
             maps = self.session.get(maps_link, allow_redirects=True)
             coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
             # print(coords)
             if coords:
               list_data['lat'] = coords[0].split(',')[0].replace('@', '')
               list_data['lon'] = coords[0].split(',')[1]
             else:
               list_data['lat'] = ''
               list_data['lon'] = ''
             list_data['Tel_no'] = card.find('dd').find_next('dd').text
             list_data['GLA'] = ''
             list_data['scrape_date'] = datetime.datetime.today().strftime("%m/%d/%Y")

             print(list_data)
             print("===========================")

             # master_list.append(list_data)
             # saveToCsv()

           if "カナート（株）" != find_qanat:  # kalo izumiya
             self.content_izumiya.append(list_data)
           else:  # kalo qanat
             self.content_qanat.append(list_data)


if __name__ == '__main__':
  SuperIzumiya(True)
