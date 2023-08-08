import re

from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import date
from fake_useragent import UserAgent
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from scraping.scraping import CleaningData
from google.cloud import storage


class SuperSunplaza(CleaningData):
  def __init__(self, from_main):
    # Taruh semua variabel yang ada di luar class/methods disini
    self.session = requests.Session()
    self.file_name = '_super_sunplaza'
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    self.session.mount("REDACTED URL" adapter)
    self.session.mount("REDACTED URL" adapter)
    self.rand_agent = {'user-agent':UserAgent().random.strip()}
    self.content=[]
    
    start_time = time.time()
    self.getdata()       
    print("--- %s seconds ---" % (time.time() - start_time))

    x = pd.DataFrame(self.content)

    # CLEANING 1: PERIKSA KOORDINAT
    x['lat'] = pd.to_numeric(x['lat'], errors='coerce')
    x['lon'] = pd.to_numeric(x['lon'], errors='coerce')
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lat'] = pd.NA
    x.loc[x[(x['lat'] < 20) | (x['lat'] > 50)].index, 'lon'] = pd.NA

    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lat'] = pd.NA
    x.loc[x[(x['lon'] < 121) | (x['lon'] > 154)].index, 'lon'] = pd.NA
    x['url_tenant'] = None

    self.df_clean = self.clean_data(x)

    if True:
      if from_main:
        self.df_clean.to_csv(f'D:/dags/csv/{self.file_name}.csv', index=False)
      else:
        client = storage.Client()
        bucket = client.get_bucket('scrapingteam')
        bucket.blob(f'/ichsan/{self.file_name}.csv').upload_from_string(
        self.df_clean.to_csv(index=False), 'text/csv')

  def __str__(self) -> str:
    return str(self.df_clean.to_html(index=False, render_links=True, max_cols=25))

  def getdata(self):
    url = "REDACTED URL"
    r = self.session.get(url, headers=self.rand_agent)
    soup = BeautifulSoup(r.content, 'html.parser')
    cards = soup.select('div.shopList__detail a')
    for card in cards:
        link = "REDACTED URL"+ card['href']
        print(link)
        page = self.session.get(link, headers=self.rand_agent)
        soup2 = BeautifulSoup(page.content, 'html.parser')
        data_dict = {}
                        
        data_dict['store_name'] = soup2.find('h2').text

        data_dict['-'] = 'サンプラザ'

        data_dict['-'] = 'SM'

        data_dict['-'] = '/super/sunplaza'

        data_dict['-'] = 'Sunplaza'

        data_dict['-'] = 'super'

        data_dict['-'] = 'ショッピング'

        data_dict['-'] = 'スーパー'

        data_dict['address'] = soup2.find('th', text=re.compile('所在地')).find_next_sibling('td').text

        data_dict['URL_store'] = link

        data_dict['URL_Tenant'] = ""

        data_dict['Open_Hours'] = soup2.find('th', text='営業時間').find_next_sibling('td').text

        data_dict['lat'] = ''
        data_dict['lon'] = ''

        data_dict['Tel_No'] = soup2.find('th', text='電話番号').find_next_sibling('td').text

        data_dict['GLA'] = 'null' 
                                                                    
        today = date.today()

        data_dict['scrape_date'] = today.strftime("%m/%d/%y")

        self.content.append(data_dict)
        # print(data_dict)
        print(len(self.content))
        print('--------------------------------------------------------------------------------')
        
if __name__ == '__main__':
  SuperSunplaza(True)