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


class DrugSigmayakuhin(CleaningData):
  def __init__(self, from_main=False):
    self.file_name = '/drug/sigmayakuhin'.replace('/', '_')
    self.from_main = from_main
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = HTMLSession()
    self.content = list()

    start = time.time()
    url = "REDACTED URL"
    self.extract(url)
    end = time.time()
    print("============ ", (end - start) / 60, " minute(s) ============")

    x = pd.DataFrame(self.content)
    x.columns = [y.lower() for y in x.columns]

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

  def extract(self, url):
    headers = {'user-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0'}
    try:
        r = self.session.get(url, headers=headers)
    except :
        raise
    soup = BeautifulSoup(r.content,'html.parser')
    cards = soup.find('div',{'class':'page-contents'}).find_all('div',{'class':'large-12 columns'})[1].find_all('div',{'class':'large-12 columns box-sha'})
    for card in cards:
        req = self.session.get(card.find('div',{'class':'large-4 medium-5 columns'}).find('a').get('href'), headers=headers)
        data = BeautifulSoup(req.content,'html.parser')
        data_dict = {}
        data_dict['Store_Name'] = data.find('h4',{'class':'stores-title'}).text
        data_dict['-'] = 'シグマ薬品'
        data_dict['CSAR Category'] = 'Drg'
        data_dict['-'] = '/drug/sigmayakuhin'
        data_dict['-'] = 'SIGMA'
        data_dict['-'] = 'drug'
        data_dict['-'] = 'ショッピング'
        data_dict['-'] = 'ドラッグストア'
        data_dict['address'] = data.find('table',{'class':'stores-table'}).find_all('tr')[0].find('td').text.replace('\r','').replace('\n', '').replace('\u3000', '')
        maps_link = "REDACTED URL"+ data_dict['address']
        maps = self.session.get(maps_link, headers=headers, allow_redirects=True)
        coords = re.findall(r'@[2-4]\d\.\d{3,},\d{3}\.\d{3,}', maps.html.html)
        # print(coords)
        if coords:
          data_dict['lat'] = coords[0].split(',')[0].replace('@', '')
          data_dict['lon'] = coords[0].split(',')[1]
        else:
          data_dict['lat'] = ''
          data_dict['lon'] = ''
        data_dict['URL_store'] = req.url
        data_dict['URL_Tenant'] = 'null'
        data_dict['Open_Hours'] = data.find('table',{'class':'stores-table'}).find_all('tr')[2].find('td').text.replace('\u3000', '').replace('\n', '')
        data_dict['Tel_No'] = data.find('table',{'class':'stores-table'}).find_all('tr')[1].find('td').text.replace('\n', '').replace('\u3000', '').replace('-', '')
        data_dict['GLA'] = ''
        data_dict['scrape_date'] = datetime.datetime.today().strftime('%m/%d/%Y')
        self.content.append(data_dict)
        # print(data_dict)



if __name__ == '__main__':
  DrugSigmayakuhin(True)
